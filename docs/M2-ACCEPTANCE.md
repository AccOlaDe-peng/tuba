# M2 集成验收记录

验收日期：2026-09-24

- 标准 OIDC verifier 通过远程 JWKS 验证签名、issuer、audience、有效期和租户声明。
- viewer、analyst、tenant_admin 权限矩阵通过单元测试。
- PostgreSQL migration 在干净实例执行成功。
- 案件创建、幂等重放、游标列表、版本冲突、指派、判定反馈均通过数据库集成测试。
- 数据库 membership 授权、即时撤权及跨租户拒绝通过集成测试。
- 案件和 membership 写操作均生成租户审计事件；审计读取游标通过测试。
- Elasticsearch 异常查询强制租户、最长 31 天时间窗口、固定字段和 `search_after` 游标。

M2 本地验收通过。共享环境仍需使用真实身份提供商和生产 PostgreSQL 拓扑执行部署验收。
