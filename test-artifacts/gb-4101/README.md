# 账单更正导入实测（gb-4101）

`run_tests.py` 针对运行中的栈执行 44 项端到端断言，覆盖需求点：乱序、同版本异内容整批拒绝与回滚、重试幂等、作废重放与终态、证据失效/未受影响保持有效、在途复核撞上更正、长复核不拖住更正、交叉并发不丢新值、相同文件并发去重、角色权限、拒绝留档可查。

## 本地复现（无 Docker 时）

1. PostgreSQL 16：zonky `embedded-postgres-binaries-linux-arm64v8` 解包，`initdb -D pgdata -U app --auth=trust`，`pg_ctl -o "-p 17740" start`，建库 `query_review`。
2. Redis 7：源码 `make CC=/usr/bin/gcc`，`redis-server --port 16379 --daemonize yes`。
3. 后端：`DATABASE_URL=postgresql+psycopg2://app@127.0.0.1:17740/query_review REDIS_URL=redis://127.0.0.1:16379/0 JWT_SECRET=local-test-secret-2026 python -m uvicorn src.main:app --port 19240`（于 `backend/`）。
4. Worker：同环境变量加 `ARTIFACTS_DIR=/tmp/devenv/artifacts python -m src.worker`。
5. 执行：`python run_tests.py`（需 psycopg2 直连库断言账单终态）。完整输出见 `test-output.log`。

Docker 环境直接 `docker compose up -d --build` 后，将脚本顶部 `BASE`/数据库端口指向映射端口即可。
