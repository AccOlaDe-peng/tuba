import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { useQueryClient } from "@tanstack/react-query";
import { principalSchema, type Principal } from "./api";

type AuthValue = {
  principal?: Principal;
  loading: boolean;
  error?: string;
  token?: string;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  can: (permission: string) => boolean;
};

const AuthContext = createContext<AuthValue | undefined>(undefined);

declare global {
  interface Window {
    TUBA_CONFIG?: { oidcIssuer?: string; oidcClientId?: string; basePath?: string };
  }
}

const issuer = (window.TUBA_CONFIG?.oidcIssuer ?? (import.meta.env.VITE_OIDC_ISSUER as string | undefined))?.replace(/\/$/, "");
const clientId = window.TUBA_CONFIG?.oidcClientId ?? (import.meta.env.VITE_OIDC_CLIENT_ID as string | undefined) ?? "tuba-web";

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [token, setToken] = useState<string | undefined>(
    () => sessionStorage.getItem("tuba.access_token") ?? undefined,
  );
  const [principal, setPrincipal] = useState<Principal>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  const clearSession = useCallback(() => {
    queryClient.clear();
    Object.keys(sessionStorage).filter(key=>key.startsWith("tuba.query-bookmarks:")).forEach(key=>sessionStorage.removeItem(key));
    sessionStorage.removeItem("tuba.access_token");
    setToken(undefined);
    setPrincipal(undefined);
  }, [queryClient]);

  const login = useCallback(async (username: string, password: string) => {
    setError(undefined);
    try {
      if (!issuer) throw new Error("未配置身份服务地址");
      const response = await fetch(`${issuer}/protocol/openid-connect/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "password",
          client_id: clientId,
          scope: "openid profile email",
          username,
          password,
        }),
      });
      if (!response.ok) {
        const rejection = await response.json().catch(() => ({})) as {error?: string};
        if (rejection.error === "Invalid origin") throw new Error("身份服务拒绝当前页面来源，请检查开发代理或登录客户端的允许来源配置。");
        if (response.status >= 500) throw new Error("身份服务暂不可用，请稍后重试。");
        if (rejection.error === "unauthorized_client" || rejection.error === "invalid_client") throw new Error("登录客户端配置不匹配，请检查身份服务配置。");
        throw new Error("用户名或密码错误，或账号暂不可登录。");
      }
      const payload = (await response.json()) as { access_token?: string };
      if (!payload.access_token) throw new Error("身份服务未返回访问令牌。");
      sessionStorage.setItem("tuba.access_token", payload.access_token);
      setToken(payload.access_token);
    } catch (reason) {
      clearSession();
      setError(reason instanceof TypeError ? "无法连接身份服务，请检查登录服务地址、网络和开发代理配置。" : reason instanceof Error ? reason.message : "登录失败，请重试。");
    }
  }, [clearSession]);

  useEffect(() => {
    const onUnauthorized = () => clearSession();
    window.addEventListener("tuba:unauthorized", onUnauthorized);
    return () => window.removeEventListener("tuba:unauthorized", onUnauthorized);
  }, [clearSession]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      setLoading(true);
      setError(undefined);
      try {
        const accessToken = token;
        if (!accessToken) return;
        const response = await fetch("/api/v1/me", {
          headers: { Authorization: `Bearer ${accessToken}` },
        });
        if (response.status === 401) {
          clearSession();
          setError("身份服务已确认登录，但 API 未接受此访问令牌（401）。");
          return;
        }
        if (!response.ok) throw new Error("无法读取当前用户权限");
        const value = principalSchema.parse(await response.json());
        if (!cancelled) setPrincipal(value);
      } catch (reason) {
        if (!cancelled) {
          clearSession();
          setError(reason instanceof Error ? reason.message : "登录失败");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [clearSession, token]);

  const value = useMemo<AuthValue>(
    () => ({
      principal,
      token,
      loading,
      error,
      login,
      logout: clearSession,
      can: (permission) => principal?.permissions.includes(permission) ?? false,
    }),
    [clearSession, error, loading, login, principal, token],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthValue {
  const value = useContext(AuthContext);
  if (!value) throw new Error("AuthProvider missing");
  return value;
}
