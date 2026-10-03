// Keep retry keys stable for an unchanged request, and renew after editing.
export class IdempotencyDraft {
  private body?: string;
  private key?: string;
  keyFor(body: string): string {
    if (body !== this.body || !this.key) {
      this.body = body;
      this.key = crypto.randomUUID();
    }
    return this.key;
  }
}

type ObjectValue = Record<string, unknown>;
function isObject(value: unknown): value is ObjectValue {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}
function validateDirectives(value: unknown): void {
  if (Array.isArray(value)) { value.forEach(validateDirectives); return; }
  if (!isObject(value)) return;
  for (const [key, child] of Object.entries(value)) {
    const name = key.trim().toLowerCase();
    const normalized = name.replace(/[_-]/g, '');
    if (name.endsWith('_env')) {
      if (typeof child !== 'string' || !/^[A-Z_][A-Z0-9_]{0,127}$/.test(child)) throw new Error('凭据环境引用必须是变量名称');
      continue;
    }
    if (['command', 'commands', 'shell', 'exec', 'executable', 'script'].includes(normalized) ||
        /password|secret|token/.test(normalized) ||
        ['apikey', 'accesskey', 'credential', 'credentials', 'privatekey', 'authorization', 'bearer'].includes(normalized)) {
      throw new Error('配置不能包含执行指令或明文凭据');
    }
    validateDirectives(child);
  }
}
export function parseManagedConfiguration(raw: string): ObjectValue {
  const config: unknown = JSON.parse(raw);
  if (!isObject(config)) throw new Error('请输入完整 JSON 配置对象');
  validateDirectives(config);
  if (config.components !== undefined && !isObject(config.components)) throw new Error('components 必须是组件配置对象');
  return config;
}
export function componentConfiguration(raw: string, component: string, version: string): ObjectValue {
  const config = parseManagedConfiguration(raw);
  const components = (config.components ?? {}) as ObjectValue;
  const existing = components[component];
  if (existing !== undefined && !isObject(existing)) throw new Error('目标组件配置必须是对象');
  return {...config, components: {...components, [component]: {...(existing ?? {}), desired_version: version}}};
}
