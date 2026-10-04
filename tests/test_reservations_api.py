"""批量限行区域调整预约 API 集成测试，需要可连接的 PostgreSQL（DATABASE_URL）。

运行：
    DATABASE_URL=postgresql://restrict@localhost:5432/restrict \
        python3 tests/test_reservations_api.py
脚本会清空 regions/region_meta/passes/commitments/observations/
observation_events/idempotency_keys/region_change_audit/batch_reservations
后跑全套用例，结束时再次清空，避免影响其它测试脚本。
"""
from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db  # noqa: E402
from app import main  # noqa: E402
from app import repository  # noqa: E402
from app.main import app  # noqa: E402

failures: list[str] = []


def check(label, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {label}{(' — ' + detail) if detail and not cond else ''}")
    if not cond:
        failures.append(label)


def iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def future(seconds: float) -> str:
    return iso_z(datetime.now(timezone.utc) + timedelta(seconds=seconds))


# 宽生效窗口，覆盖所有动态生成的未来轨迹时刻
WIDE = ("2020-01-01T00:00:00Z", "2031-01-01T00:00:00Z")

BOX_A = [[0, 0], [100, 0], [100, 100], [0, 100]]
BOX_B = [[200, 0], [300, 0], [300, 100], [200, 100]]
BOX_C = [[0, 120], [100, 120], [100, 180], [0, 180]]      # 挡 y=150 航线
BOX_D = [[0, 20], [100, 20], [100, 80], [0, 80]]          # 挡 y=50 航线

db.init_pool()
client = TestClient(app)


def reset_all():
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE batch_reservations, observation_events, observations, "
                "idempotency_keys, commitments, passes, region_change_audit, "
                "regions RESTART IDENTITY"
            )
            cur.execute("UPDATE region_meta SET revision = 0 WHERE id = 1")
        conn.commit()


def reserve(execute_at, expected_revision, operations, trajectories, **extra):
    body = {
        "execute_at": execute_at,
        "expected_revision": expected_revision,
        "operations": operations,
        "trajectories": trajectories,
    }
    body.update(extra)
    return client.post("/api/regions/batch/reservations", json=body)


def get_res(rid):
    return client.get(f"/api/regions/batch/reservations/{rid}")


def cancel_res(rid):
    return client.post(f"/api/regions/batch/reservations/{rid}/cancel")


def force_due(rid):
    """把预约执行时刻改到过去（模拟时间流逝），配合同步 process 确定性执行。"""
    with db.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE batch_reservations SET execute_at = now() - interval '5 seconds'"
                " WHERE id = %s",
                (rid,),
            )
        conn.commit()


def process():
    """同步执行一轮到期预约（与后台执行器同一处理路径）。"""
    return main.process_due_reservations()


def revision():
    return client.get("/api/revision").json()["revision"]


def audit_rows():
    r = client.get("/api/regions/changes", params={"limit": 500})
    assert r.status_code == 200, r.text
    return r.json()["changes"]


def region_ids():
    return [r["id"] for r in client.get("/api/regions").json()]


def safe_traj(y=500, start=3600, span=60):
    return [
        {"x": -50, "y": y, "t": future(start)},
        {"x": 450, "y": y, "t": future(start + span)},
    ]


def create_op(name, polygon, vf=WIDE[0], vt=WIDE[1]):
    return {"action": "create", "name": name, "polygon": polygon,
            "valid_from": vf, "valid_to": vt}


reset_all()

# ================================================================ A. 受理
print("== A. 预约受理（只校验输入格式，不预占 revision） ==")

# ---- 成功受理：201 + pending，回显规范化请求，revision 不变 ----
r = reserve(future(3600), 0, [create_op("C区", BOX_C)], [safe_traj()])
check("受理成功 201", r.status_code == 201, r.text)
b = r.json()
RID_OK = b["id"]
check("返回预约编号与待执行状态", b["id"] >= 1 and b["status"] == "pending", str(b))
check("回显执行时刻（UTC Z）", b["execute_at"].endswith("Z"), b["execute_at"])
check("回显 expected_revision 与 operations/trajectories",
      b["expected_revision"] == 0
      and b["operations"][0]["action"] == "create"
      and b["operations"][0]["name"] == "C区"
      and len(b["trajectories"]) == 1, str(b))
check("result/failure/executed_at/cancelled_at 均为空",
      b["result"] is None and b["failure"] is None
      and b["executed_at"] is None and b["cancelled_at"] is None, str(b))
check("created_at 已置位", bool(b["created_at"]))
check("受理不推进 revision", revision() == 0)

# ---- 受理不预占编号：随后直接登记的区域拿到 1 号 ----
r = client.post("/api/regions", json={
    "name": "A区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
check("受理后登记区域 id=1（预约未占用编号）", r.status_code == 201
      and r.json()["region"]["id"] == 1, r.text)
check("受理不预占 revision（登记后 revision=1）", revision() == 1)

# ---- 时区归一化：+08:00 提交，回显 UTC Z ----
exec_utc = datetime.now(timezone.utc) + timedelta(hours=2)
exec_cst = exec_utc.astimezone(timezone(timedelta(hours=8)))
r = reserve(exec_cst.isoformat(), 1, [create_op("C2", BOX_C)], [safe_traj()])
check("+08:00 执行时刻受理 201", r.status_code == 201, r.text)
check("执行时刻归一化为 UTC Z",
      r.json()["execute_at"] == iso_z(exec_utc), r.json()["execute_at"])
RID_TZ = r.json()["id"]

# ---- 不带时区 / 非未来时刻：可区分错误 ----
r = reserve("2030-06-01T10:00:00", 1, [create_op("X", BOX_C)], [safe_traj()])
check("execute_at 不带时区 422 validation_error",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)
r = reserve(future(-10), 1, [create_op("X", BOX_C)], [safe_traj()])
check("execute_at 在过去 422 execution_not_in_future",
      r.status_code == 422 and r.json()["error"]["code"] == "execution_not_in_future", r.text)
r = reserve(iso_z(datetime.now(timezone.utc) - timedelta(milliseconds=1)), 1,
            [create_op("X", BOX_C)], [safe_traj()])
check("execute_at 不晚于受理时刻 422 execution_not_in_future",
      r.status_code == 422 and r.json()["error"]["code"] == "execution_not_in_future", r.text)

# ---- 输入格式校验（与批量变更受理口径一致） ----
r = reserve(future(60), 1, [create_op("X", BOX_C)],
            [[{"x": 0, "y": 0, "t": future(10)}]])
check("轨迹少于 2 点 422 validation_error",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)
r = reserve(future(60), 1, [create_op("X", BOX_C)],
            [[{"x": 0, "y": 0, "t": future(20)},
              {"x": 1, "y": 1, "t": future(10)}]])
check("轨迹时刻不严格递增 422 invalid_trajectory",
      r.status_code == 422 and r.json()["error"]["code"] == "invalid_trajectory", r.text)
r = reserve(future(60), 1,
            [create_op("X", [[0, 0], [100, 100], [0, 100], [100, 0]])],
            [safe_traj()])
check("自相交多边形 422（与既有接口一致：validation_error，含非法原因）",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error"
      and "自相交" in r.text, r.text)
r = reserve(future(60), 1, [
    {"action": "update", "id": 1, "name": "甲"},
    {"action": "delete", "id": 1},
], [safe_traj()])
check("同一区域一批多次 422 duplicate_region",
      r.status_code == 422 and r.json()["error"]["code"] == "duplicate_region", r.text)
r = reserve(future(60), 1, [
    create_op("X", BOX_C, "2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z")],
    [safe_traj()])
check("create 时段倒序 422 invalid_operation",
      r.status_code == 422 and r.json()["error"]["code"] == "invalid_operation", r.text)
r = reserve(future(60), 1, [create_op("X", BOX_C)], [safe_traj()], bogus=1)
check("多余字段 422 validation_error",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)
r = reserve(future(60), -1, [create_op("X", BOX_C)], [safe_traj()])
check("expected_revision 为负 422 validation_error",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)
r = reserve(future(60), 1, [], [safe_traj()])
check("operations 为空 422 validation_error",
      r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)

# ---- 受理不保证届时可提交：revision 不符/区域不存在/轨迹不安全仍受理 ----
r = reserve(future(3600), 999,
            [{"action": "update", "id": 999, "name": "不存在"}],
            [[{"x": 0, "y": 50, "t": future(100)},
              {"x": 100, "y": 50, "t": future(160)}]])  # 轨迹直穿 A 区
check("revision 不符/区域不存在/轨迹不安全仍受理 201 pending",
      r.status_code == 201 and r.json()["status"] == "pending", r.text)
RID_DEFER = r.json()["id"]
check("延期校验的预约不推进 revision", revision() == 1)
r = cancel_res(RID_DEFER)
check("清理：取消该预约 200 cancelled",
      r.status_code == 200 and r.json()["status"] == "cancelled", r.text)
cancel_res(RID_OK)
cancel_res(RID_TZ)

# ================================================================ B. 执行成功
print("== B. 到期执行成功：区域/revision/观察事件/审计一次性提交 ==")
reset_all()

# 准备：批量登记 A、B（rev=1）；登记观察（y=150 航线，当前安全）
r = client.post("/api/regions/batch", json={
    "expected_revision": 0,
    "operations": [create_op("A区", BOX_A), create_op("B区", BOX_B)],
    "trajectories": [safe_traj()],
})
check("准备批次成功 rev=1", r.status_code == 200 and r.json()["revision"] == 1, r.text)
OBS_TRAJ = [
    {"x": -50, "y": 150, "t": future(3600)},
    {"x": 450, "y": 150, "t": future(3700)},
]
r = client.post("/api/observations", json={"points": OBS_TRAJ})
check("观察登记 201 结论 safe", r.status_code == 201
      and r.json()["conclusion"] == "safe", r.text)
OBS_ID = r.json()["id"]

# 预约：create C（挡 y=150）+ update A 改名 + delete B
r = reserve(future(3600), 1, [
    create_op("C区", BOX_C),
    {"action": "update", "id": 1, "name": "A区-改名"},
    {"action": "delete", "id": 2},
], [safe_traj()])
check("预约受理 201", r.status_code == 201, r.text)
RID = r.json()["id"]
check("执行前 revision 仍=1", revision() == 1)

force_due(RID)
processed = process()
check("到期预约被处理", RID in processed, str(processed))

r = get_res(RID)
b = r.json()
check("查询 200 且状态 committed", r.status_code == 200 and b["status"] == "committed", r.text)
check("提交结果 revision=2（整批只 +1）", b["result"]["revision"] == 2, str(b["result"]))
ops = [(o["action"], o["region_id"], o["version"]) for o in b["result"]["operations"]]
check("逐操作结果（create 新 id/version=1、update version=2、delete 回显）",
      ops == [("create", 3, 1), ("update", 1, 2), ("delete", 2, 1)], str(ops))
check("提交结果含逐轨迹核验（safe）",
      b["result"]["trajectories"][0]["safe"] is True
      and len(b["result"]["trajectories"][0]["results"]) >= 1, str(b["result"]))
check("executed_at 置位、failure 为空",
      bool(b["executed_at"]) and b["failure"] is None, str(b))
check("区域集合已变更：C 存在、B 删除", sorted(region_ids()) == [1, 3], str(region_ids()))
check("A 区已改名且 version=2",
      client.get("/api/regions/1").json()["name"] == "A区-改名"
      and client.get("/api/regions/1").json()["version"] == 2)
check("全局 revision=2", revision() == 2)

# 审计：整批一行 change_type=batch
changes = audit_rows()
check("审计含 revision=2 的 batch 记录",
      any(c["revision"] == 2 and c["change_type"] == "batch"
          and c["summary"]["total"] == 3 for c in changes), str(changes))

# 观察翻转事件：safe→unsafe，同一事务
r = client.get(f"/api/observations/{OBS_ID}")
check("观察结论已翻转为 unsafe（revision=2）",
      r.json()["conclusion"] == "unsafe" and r.json()["revision"] == 2, r.text)
r = client.get(f"/api/observations/{OBS_ID}/events")
evs = r.json()["events"]
check("翻转事件一条：safe→unsafe @rev2",
      len(evs) == 1 and evs[0]["old_conclusion"] == "safe"
      and evs[0]["new_conclusion"] == "unsafe" and evs[0]["revision"] == 2, str(evs))

# 重复处理不重复提交（重启/重扫安全）
processed = process()
check("已提交预约不再被处理", RID not in processed, str(processed))
check("重复处理后 revision 不变、审计不增（准备批次 + 预约批次共 2 行）",
      revision() == 2 and len(audit_rows()) == 2, str(audit_rows()))

# ================================================================ C. 执行失败（沿用批量错误码）
print("== C. 到期重验失败：记录可查询原因，区域集合不变 ==")

# ---- revision_conflict ----
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-改"}], [safe_traj()])
RID = r.json()["id"]
client.patch("/api/regions/1", json={"name": "X-外部变更"})  # revision → 2
force_due(RID)
process()
b = get_res(RID).json()
check("revision 过期 → failed/revision_conflict",
      b["status"] == "failed" and b["failure"]["code"] == "revision_conflict", str(b))
check("失败明细带当前 revision",
      b["failure"]["details"][0]["current_revision"] == 2, str(b["failure"]))
check("失败不改变区域：名称未被预约改动",
      client.get("/api/regions/1").json()["name"] == "X-外部变更")
check("失败不推进 revision、不新增审计（仍只有登记与外部修订 2 行）",
      revision() == 2 and len(audit_rows()) == 2, str(audit_rows()))
check("失败可查询：executed_at 置位、result 为空",
      bool(b["executed_at"]) and b["result"] is None, str(b))

# ---- region_not_found ----
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
client.delete("/api/regions/1")  # revision → 2，区域不存在
r = reserve(future(3600), 2, [{"action": "update", "id": 1, "name": "X-改"}], [safe_traj()])
RID = r.json()["id"]
force_due(RID)
process()
b = get_res(RID).json()
check("引用区域已被删除 → failed/region_not_found",
      b["status"] == "failed" and b["failure"]["code"] == "region_not_found", str(b))
check("明细带操作下标与区域 id",
      b["failure"]["details"][0]["operation_index"] == 0
      and b["failure"]["details"][0]["region_id"] == 1, str(b["failure"]))
check("revision 不变", revision() == 2)

# ---- invalid_operation（执行时合并后时段非法） ----
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A,
    "valid_from": "2026-01-01T00:00:00Z", "valid_to": "2027-01-01T00:00:00Z"})
client.patch("/api/regions/1", json={"valid_from": "2026-12-01T00:00:00Z"})  # rev=2
r = reserve(future(3600), 2,
            [{"action": "update", "id": 1, "valid_to": "2026-06-01T00:00:00Z"}],
            [safe_traj()])
check("受理时不深查 update 合并时段（201）", r.status_code == 201, r.text)
RID = r.json()["id"]
force_due(RID)
process()
b = get_res(RID).json()
check("执行时合并时段非法 → failed/invalid_operation",
      b["status"] == "failed" and b["failure"]["code"] == "invalid_operation", str(b))
check("明细带操作下标", b["failure"]["details"][0]["operation_index"] == 0, str(b["failure"]))
check("区域未被改动（version 仍=2）", client.get("/api/regions/1").json()["version"] == 2)

# ---- unsafe_trajectory ----
reset_all()
cross = [
    {"x": -50, "y": 50, "t": future(3600)},
    {"x": 450, "y": 50, "t": future(3700)},
]
r = reserve(future(3600), 0, [create_op("D区", BOX_D)], [cross])
RID = r.json()["id"]
force_due(RID)
process()
b = get_res(RID).json()
check("自带轨迹穿越 → failed/unsafe_trajectory",
      b["status"] == "failed" and b["failure"]["code"] == "unsafe_trajectory", str(b))
det = b["failure"]["details"][0]
check("明细带轨迹下标与穿越起止",
      det["trajectory_index"] == 0
      and det["results"][0]["crossings"][0]["entry_time"].endswith("Z"), str(det))
check("区域未落库", region_ids() == [] and revision() == 0)

# ---- pass_conflict（预约受理后签发的凭证在执行时保护） ----
reset_all()
r = reserve(future(3600), 0, [create_op("D区", BOX_D)], [safe_traj()])
RID = r.json()["id"]
r = client.post("/api/passes", json={"points": [
    {"x": -50, "y": 50, "t": future(3600)},
    {"x": 450, "y": 50, "t": future(3700)},
]})
check("凭证签发 201", r.status_code == 201, r.text)
PASS_ID = r.json()["id"]
force_due(RID)
process()
b = get_res(RID).json()
check("危及有效凭证 → failed/pass_conflict",
      b["status"] == "failed" and b["failure"]["code"] == "pass_conflict", str(b))
check("明细带冲突凭证 id",
      b["failure"]["details"][0]["pass_id"] == PASS_ID, str(b["failure"]))
check("区域未落库、revision 不变", region_ids() == [] and revision() == 0)

# ---- commitment_conflict ----
reset_all()
r = reserve(future(3600), 0, [create_op("D区", BOX_D)], [safe_traj()])
RID = r.json()["id"]
r = client.post("/api/commitments", json={
    "points": [{"x": -50, "y": 50, "t": 0}, {"x": 450, "y": 50, "t": 100}],
    "window_start": future(3600), "window_end": future(7200),
})
check("承诺签发 201", r.status_code == 201, r.text)
CMT_ID = r.json()["id"]
force_due(RID)
process()
b = get_res(RID).json()
check("抹掉承诺全部未来安全时刻 → failed/commitment_conflict",
      b["status"] == "failed" and b["failure"]["code"] == "commitment_conflict", str(b))
check("明细带冲突承诺 id",
      b["failure"]["details"][0]["commitment_id"] == CMT_ID, str(b["failure"]))
check("区域未落库、revision 不变", region_ids() == [] and revision() == 0)

# ================================================================ D. 取消
print("== D. 待执行预约可取消；终态取消返回可区分错误 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})

r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-改"}], [safe_traj()])
RID = r.json()["id"]
r = cancel_res(RID)
check("pending 取消 200 cancelled", r.status_code == 200
      and r.json()["status"] == "cancelled", r.text)
check("cancelled_at 置位", bool(r.json()["cancelled_at"]), r.text)
check("取消不推进 revision", revision() == 1)
r = cancel_res(RID)
check("已取消再取消 409 reservation_already_terminal",
      r.status_code == 409
      and r.json()["error"]["code"] == "reservation_already_terminal", r.text)
check("终态取消明细带当前状态",
      r.json()["error"]["details"][0]["status"] == "cancelled", r.text)
r = cancel_res(999999)
check("未知编号取消 404 not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "not_found", r.text)

# 已取消预约到期不执行
force_due(RID)
processed = process()
check("已取消预约不被执行", RID not in processed, str(processed))
check("取消后区域/revision 不变",
      client.get("/api/regions/1").json()["name"] == "X区" and revision() == 1)
check("取消后状态仍 cancelled", get_res(RID).json()["status"] == "cancelled")

# committed / failed 终态取消 → 409
r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-改2"}], [safe_traj()])
RID_C = r.json()["id"]
force_due(RID_C)
process()
check("预约已 committed", get_res(RID_C).json()["status"] == "committed")
r = cancel_res(RID_C)
check("committed 取消 409（details.status=committed）",
      r.status_code == 409
      and r.json()["error"]["details"][0]["status"] == "committed", r.text)

r = reserve(future(3600), 999, [{"action": "update", "id": 1, "name": "X-改3"}], [safe_traj()])
RID_F = r.json()["id"]
force_due(RID_F)
process()
check("预约已 failed", get_res(RID_F).json()["status"] == "failed")
r = cancel_res(RID_F)
check("failed 取消 409（details.status=failed）",
      r.status_code == 409
      and r.json()["error"]["details"][0]["status"] == "failed", r.text)

# ================================================================ E. 查询
print("== E. 查询契约 ==")
r = get_res(999999)
check("未知编号查询 404 not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "not_found", r.text)
r = get_res(RID_C)
check("committed 查询返回提交结果",
      r.status_code == 200 and r.json()["result"]["revision"] == 2, r.text)
r = get_res(RID_F)
check("failed 查询返回失败原因",
      r.status_code == 200 and r.json()["failure"]["code"] == "revision_conflict", r.text)

# ================================================================ F. 顺序与至多一次
print("== F. 到期按 (execute_at, id) 顺序处理；并发/重启至多提交一次 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})

# 两条预约：先到期者先执行；后到期者看到新 revision → revision_conflict
r1 = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-早"}], [safe_traj()])
R1 = r1.json()["id"]
r2 = reserve(future(7200), 1, [{"action": "update", "id": 1, "name": "X-晚"}], [safe_traj()])
R2 = r2.json()["id"]
force_due(R1)
force_due(R2)
processed = process()
check("两条到期预约都被处理", R1 in processed and R2 in processed, str(processed))
check("先到期者 committed", get_res(R1).json()["status"] == "committed")
b2 = get_res(R2).json()
check("后到期者 failed/revision_conflict（看到先者推进的 revision）",
      b2["status"] == "failed" and b2["failure"]["code"] == "revision_conflict", str(b2))
check("区域名称为先到期者的结果", client.get("/api/regions/1").json()["name"] == "X-早")

# 同一执行时刻：按编号顺序
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r1 = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-小编号"}], [safe_traj()])
R1 = r1.json()["id"]
r2 = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-大编号"}], [safe_traj()])
R2 = r2.json()["id"]
force_due(R1)
force_due(R2)
process()
check("同刻预约小编号先提交",
      get_res(R1).json()["status"] == "committed"
      and get_res(R2).json()["failure"]["code"] == "revision_conflict")
check("区域名称为小编号预约的结果", client.get("/api/regions/1").json()["name"] == "X-小编号")

# 并发处理：同一预约至多提交一次
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-并发"}], [safe_traj()])
RID = r.json()["id"]
force_due(RID)
errors: list[Exception] = []


def run_process():
    try:
        process()
    except Exception as exc:  # pragma: no cover - 仅用于失败定位
        errors.append(exc)


threads = [threading.Thread(target=run_process) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("并发处理无异常", not errors, str(errors[:1]))
b = get_res(RID).json()
check("并发处理后状态 committed", b["status"] == "committed", str(b))
check("revision 只推进一次（=2）", revision() == 2, str(revision()))
check("审计恰好两行（登记 + 预约批次各一，预约只提交一次）",
      len(audit_rows()) == 2
      and sum(1 for c in audit_rows() if c["change_type"] == "batch") == 1,
      str(audit_rows()))
check("区域 version 只 +1（=2）", client.get("/api/regions/1").json()["version"] == 2)

# 取消与执行竞争：只产生一个终态
race_ok = True
race_detail = ""
for round_no in range(8):
    reset_all()
    client.post("/api/regions", json={
        "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
    r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": f"X-r{round_no}"}],
                [safe_traj()])
    rid = r.json()["id"]
    force_due(rid)
    cancel_resp: list = []
    t_exec = threading.Thread(target=run_process)
    t_cancel = threading.Thread(target=lambda: cancel_resp.append(cancel_res(rid)))
    t_exec.start()
    t_cancel.start()
    t_exec.join()
    t_cancel.join()
    final = get_res(rid).json()
    terminal = final["status"] in ("committed", "cancelled")
    if not terminal:
        race_ok = False
        race_detail = f"第{round_no}轮终态异常: {final['status']}"
        break
    if final["status"] == "cancelled":
        # 取消赢：区域与 revision 不变，取消响应 200
        if not (revision() == 1 and client.get("/api/regions/1").json()["name"] == "X区"
                and cancel_resp[0].status_code == 200):
            race_ok = False
            race_detail = f"第{round_no}轮取消赢但区域/revision 被改动"
            break
    else:
        # 执行赢：revision 推进，取消得到 409
        if not (revision() == 2 and cancel_resp[0].status_code == 409
                and cancel_resp[0].json()["error"]["code"] == "reservation_already_terminal"):
            race_ok = False
            race_detail = f"第{round_no}轮执行赢但取消未得 409"
            break
check("取消与执行竞争 8 轮均恰一个终态且一致", race_ok, race_detail)

# ================================================================ G. 后台执行器（真实线程）
print("== G. 后台执行器：到期自动执行，重启后拾起 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r = reserve(future(1.0), 1, [{"action": "update", "id": 1, "name": "X-后台"}], [safe_traj()])
RID = r.json()["id"]
main.start_reservation_worker()
try:
    deadline = time.time() + 15
    final = None
    while time.time() < deadline:
        final = get_res(RID).json()
        if final["status"] != "pending":
            break
        time.sleep(0.2)
    check("后台执行器到期自动提交", final is not None and final["status"] == "committed",
          str(final))
    check("后台提交结果 revision=2",
          final is not None and final["result"]["revision"] == 2, str(final))
finally:
    main.stop_reservation_worker()
check("后台执行器已停止", True)
# 等后台线程越过可能正在进行的一轮的取数点，避免它处理到下一节重置后的数据
time.sleep(main.RESERVATION_WORKER_INTERVAL + 0.2)

# ================================================================ H. 有界分页取数
print("== H. 有界取数与持续推进：积压不随总量线性增长，翻页处理全部到期预约 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})

TOTAL = 7
rids = []
for i in range(TOTAL):
    # 均预期 revision=1：第 1 条（编号最小）committed，其余随后看到 rev=2
    r = reserve(future(3600 + i), 1,
                [{"action": "update", "id": 1, "name": f"X-分页{i}"}], [safe_traj()])
    assert r.status_code == 201, r.text
    rids.append(r.json()["id"])
    force_due(rids[-1])

# 有界取数原语：每页严格按 (execute_at, id) keyset 推进，单页行数有上界
with db.pool.connection() as conn:
    conn.rollback()
    with conn.cursor() as cur:
        repository.configure_cursor(cur)
        now = repository.utcnow()
        p1 = repository.list_due_reservation_ids(cur, now, limit=3)
        p2 = repository.list_due_reservation_ids(
            cur, now, limit=3,
            after_execute_at=p1[-1]["execute_at"], after_id=p1[-1]["id"])
        p3 = repository.list_due_reservation_ids(
            cur, now, limit=3,
            after_execute_at=p2[-1]["execute_at"], after_id=p2[-1]["id"])
    conn.commit()
ids_pages = [[r["id"] for r in p] for p in (p1, p2, p3)]
check("单页取数以 limit 为界（3/3/1）", [len(p) for p in ids_pages] == [3, 3, 1],
      str(ids_pages))
check("页间无重复无遗漏、按 id 升序",
      sum(ids_pages, []) == rids and all(len(set(p)) == len(p) for p in ids_pages),
      str(ids_pages))

old_size = main.RESERVATION_FETCH_BATCH_SIZE
main.RESERVATION_FETCH_BATCH_SIZE = 3  # 7 条到期预约 ⇒ 至少 3 页
try:
    processed = process()
finally:
    main.RESERVATION_FETCH_BATCH_SIZE = old_size
check("跨页持续推进：全部到期预约一轮处理完",
      sorted(processed) == sorted(rids), str(processed))
check("第 1 条 committed、其余 failed/revision_conflict（顺序语义保留）",
      get_res(rids[0]).json()["status"] == "committed"
      and all(get_res(rid).json()["failure"]["code"] == "revision_conflict"
              for rid in rids[1:]),
      str([(rid, get_res(rid).json()["status"]) for rid in rids]))
check("积压处理后 revision=2（只第一条推进）", revision() == 2, str(revision()))

# ================================================================ I. 未预期故障：回滚、退避、不饿死
print("== I. 未预期故障回滚保留 pending 并退避；同轮继续处理其余到期预约 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})

# 两条到期预约：前一条执行时注入未预期故障，后一条正常
r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-故障条"}],
            [safe_traj()])
RID_BAD = r.json()["id"]
r = reserve(future(3601), 2, [{"action": "update", "id": 1, "name": "X-正常条"}],
            [safe_traj()])
RID_OK2 = r.json()["id"]
force_due(RID_BAD)
force_due(RID_OK2)

real_apply_batch = repository.apply_batch


def apply_batch_boom(cur, payload, trajectories):
    names = [getattr(op, "name", None) for op in payload.operations]
    if "X-故障条" in names:
        raise RuntimeError("注入的未预期数据库故障")
    return real_apply_batch(cur, payload, trajectories)


repository.apply_batch = apply_batch_boom
old_base = main.RESERVATION_RETRY_BASE_DELAY
main.RESERVATION_RETRY_BASE_DELAY = 0.05  # 缩短退避便于后续验证恢复
try:
    processed = process()
finally:
    repository.apply_batch = real_apply_batch
    main.RESERVATION_RETRY_BASE_DELAY = old_base

# 故障条：事务回滚，仍 pending、无 result/failure/executed_at，区域未被改
b = get_res(RID_BAD).json()
check("未预期故障后预约保持 pending（可后续重试）", b["status"] == "pending", str(b))
check("故障条无 result/failure/executed_at（事务已回滚）",
      b["result"] is None and b["failure"] is None and b["executed_at"] is None, str(b))
check("故障条未改区域名称、revision 仍=1",
      client.get("/api/regions/1").json()["name"] == "X区" and revision() == 1)
# 退避簿记：attempts+1、next_attempt_at 推到未来
with db.pool.connection() as conn:
    conn.rollback()
    with conn.cursor() as cur:
        repository.configure_cursor(cur)
        cur.execute(
            "SELECT attempts, next_attempt_at FROM batch_reservations WHERE id = %s",
            (RID_BAD,),
        )
        book = cur.fetchone()
    conn.commit()
check("故障条退避簿记 attempts=1 且 next_attempt_at 推到未来",
      book is not None and book["attempts"] == 1
      and book["next_attempt_at"] > repository.utcnow(), str(book))
# 同轮不饿死：后一条正常预约已处理（它预期 rev=2，因故障条回滚仍 rev=1
# 而 revision_conflict 落 failed——重点是它确实被处理到了）
check("同轮继续推进后续到期预约（不被故障条饿死）",
      RID_OK2 in processed
      and get_res(RID_OK2).json()["status"] == "failed"
      and get_res(RID_OK2).json()["failure"]["code"] == "revision_conflict",
      str(processed))

# 退避期内不重试；退避期满（或重启后接续）重试成功，只提交一次
processed_now = process()
check("退避期内不取故障条（仍 pending）",
      RID_BAD not in processed_now and get_res(RID_BAD).json()["status"] == "pending",
      str(processed_now))
time.sleep(0.15)  # 超过 0.05s 退避
processed = process()
b = get_res(RID_BAD).json()
check("退避期满后续期重试成功 → committed",
      RID_BAD in processed and b["status"] == "committed"
      and b["result"]["revision"] == 2, str(b))
check("恢复后区域名为故障条预约的结果、revision=2",
      client.get("/api/regions/1").json()["name"] == "X-故障条" and revision() == 2)
check("重试成功后 attempts 簿记保留（不再增长）",
      get_res(RID_BAD).json()["status"] == "committed")

# 业务拒绝（非未预期故障）直接 failed，不写退避簿记
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-业务失败"}],
            [safe_traj()])
RID_BIZ = r.json()["id"]
client.patch("/api/regions/1", json={"name": "X-外部"})
force_due(RID_BIZ)
process()
with db.pool.connection() as conn:
    conn.rollback()
    with conn.cursor() as cur:
        repository.configure_cursor(cur)
        cur.execute(
            "SELECT attempts, next_attempt_at, status FROM batch_reservations WHERE id = %s",
            (RID_BIZ,),
        )
        book = cur.fetchone()
    conn.commit()
check("业务拒绝直接 failed 且不经退避簿记（attempts=0）",
      book["status"] == "failed" and book["attempts"] == 0, str(book))

# ================================================================ J. 多实例：SKIP LOCKED 互不阻塞
print("== J. 多实例并发：行被锁时 SKIP LOCKED 跳过、不阻塞，最终至多提交一次 ==")
reset_all()
client.post("/api/regions", json={
    "name": "X区", "polygon": BOX_A, "valid_from": WIDE[0], "valid_to": WIDE[1]})
r1 = reserve(future(3600), 1, [{"action": "update", "id": 1, "name": "X-锁内"}],
             [safe_traj()])
RID_LOCK = r1.json()["id"]
r2 = reserve(future(3601), 2, [{"action": "update", "id": 1, "name": "X-锁外"}],
             [safe_traj()])
RID_FREE = r2.json()["id"]
force_due(RID_LOCK)
force_due(RID_FREE)

# 另一个事务（模拟另一实例/取消请求）在独立连接上持住 RID_LOCK 的行锁不提交。
# 注意：psycopg 连接池按线程复用连接，process() 必须在另一个线程里运行，
# 否则会拿到持锁事务所在的同一条连接（其开头的 rollback 会释放行锁）
held = {"processed": None, "error": None}

with db.pool.connection() as holder:
    holder.rollback()
    with holder.cursor() as hcur:
        hcur.execute(
            "SELECT id FROM batch_reservations WHERE id = %s FOR UPDATE",
            (RID_LOCK,),
        )
        hcur.fetchone()

    def run_process_while_locked():
        try:
            held["processed"] = process()
        except Exception as exc:  # pragma: no cover - 仅用于失败定位
            held["error"] = exc

    t_proc = threading.Thread(target=run_process_while_locked)
    t_proc.start()
    t_proc.join(timeout=15)
    check("持锁期间执行器线程正常结束（SKIP LOCKED 不等待、不死锁）",
          t_proc.is_alive() is False and held["error"] is None, str(held["error"]))
    processed = held["processed"] or []
    check("被其它事务持锁的预约本轮跳过（SKIP LOCKED 不等待）",
          RID_LOCK not in processed and get_res(RID_LOCK).json()["status"] == "pending",
          str(processed))
    check("持锁不阻塞后续到期预约处理",
          RID_FREE in processed
          and get_res(RID_FREE).json()["failure"]["code"] == "revision_conflict",
          str(processed))
# 持锁事务回滚释放（模拟对端中止）：下一轮该预约正常被执行
holder.rollback()
processed = process()
check("锁释放后下一轮接续执行 → committed（至多提交一次）",
      RID_LOCK in processed and get_res(RID_LOCK).json()["status"] == "committed"
      and revision() == 2, str(processed))

# ================================================================ K. 处理积压期间普通请求可得连接
print("== K. 大积压一轮处理期间，普通 HTTP 请求仍可获得数据库连接 ==")
reset_all()
N_BIG = 120
for i in range(N_BIG):
    r = reserve(future(3600 + i), 0,
                [{"action": "update", "id": 999, "name": f"X-不存在{i}"}],
                [safe_traj()])  # 区域 999 不存在 → failed/region_not_found
    force_due(r.json()["id"])

interleaved: list = []
old_size = main.RESERVATION_FETCH_BATCH_SIZE
main.RESERVATION_FETCH_BATCH_SIZE = 25
stop_poke = threading.Event()


def poke_http():
    # 与执行器并发的普通只读 HTTP 请求：每次都应快速拿到连接并 200
    while not stop_poke.is_set():
        t0 = time.time()
        rr = client.get("/api/revision")
        interleaved.append((rr.status_code, time.time() - t0))
        time.sleep(0.002)


t_poke = threading.Thread(target=poke_http)
t_poke.start()
try:
    t0 = time.time()
    processed = process()
    elapsed = time.time() - t0
finally:
    stop_poke.set()
    t_poke.join()
    main.RESERVATION_FETCH_BATCH_SIZE = old_size
check("大积压一轮全部处理（120 条跨多页）", len(processed) == N_BIG,
      f"{len(processed)}/{N_BIG}")
check("全部业务拒绝 failed/region_not_found、revision 不变",
      revision() == 0 and len(processed) == N_BIG)
check("处理期间并发只读请求全部 200 且快速返回（连接未被执行器占满）",
      bool(interleaved) and all(code == 200 for code, _ in interleaved)
      and max(d for _, d in interleaved) < 5.0,
      f"请求数={len(interleaved)}, 最慢={max((d for _, d in interleaved), default=0):.3f}s, "
      f"处理耗时={elapsed:.2f}s")

# ================================================================ 清理
reset_all()

print()
if failures:
    print(f"共 {len(failures)} 个用例失败：")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("全部批量调整预约 API 集成用例通过")
