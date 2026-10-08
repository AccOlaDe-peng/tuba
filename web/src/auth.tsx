import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { principalSchema, type Principal } from "./api";

type AuthValue = {
 principal?: Principal;
 loading: boolean;
 error?: string;
 // Existing API callers may accept a bearer token for non-browser clients.
 // Browser authentication uses the HttpOnly session cookie exclusively.
 token?: string;
 login: (username: string, password: string) => Promise<void>;
 logout: () => Promise<void>;
 changePassword: (current: string, password: string) => Promise<void>;
 can: (permission: string) => boolean;
};
const AuthContext = createContext<AuthValue | undefined>(undefined);
declare global { interface Window { TUBA_CONFIG?: {basePath?: string}; } }

export function AuthProvider({children}: {children: ReactNode}) {
 const queryClient = useQueryClient();
 const [principal, setPrincipal] = useState<Principal>();
 const [loading, setLoading] = useState(true);
 const [error, setError] = useState<string>();
 const clearSession = useCallback(() => {
  queryClient.clear();
  Object.keys(sessionStorage).filter(key => key.startsWith("tuba.query-bookmarks:")).forEach(key => sessionStorage.removeItem(key));
  sessionStorage.removeItem("tuba.access_token");
  setPrincipal(undefined);
 }, [queryClient]);

 const login = useCallback(async (username: string, password: string) => {
  setError(undefined);
  try {
   const response = await fetch("/api/v1/auth/login", {
    method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({username, password}),
   });
   if (!response.ok) {
    if (response.status === 429) throw new Error("登录尝试过多，请稍后重试。");
    if (response.status >= 500) throw new Error("系统登录暂不可用，请稍后重试。");
    if (response.status === 403) throw new Error("系统拒绝当前页面来源，请检查访问地址。");
    throw new Error("用户名或密码错误，或账号暂不可登录。");
   }
   const value = principalSchema.parse(await response.json());
   queryClient.clear();
   setPrincipal(value);
  } catch (reason) {
   clearSession();
   setError(reason instanceof TypeError ? "无法连接系统，请检查网络连接。" : reason instanceof Error ? reason.message : "登录失败，请重试。");
  }
 }, [clearSession, queryClient]);

 const logout = useCallback(async () => {
  try {
   const response = await fetch("/api/v1/auth/logout", {method: "POST", credentials: "same-origin"});
   if (!response.ok) throw new Error("退出失败，请检查网络后重试。");
   clearSession(); setError(undefined);
  } catch (reason) {setError(reason instanceof Error ? reason.message : "退出失败");}
 }, [clearSession]);

 const changePassword = useCallback(async (current: string, password: string) => {
  const response = await fetch("/api/v1/auth/password", {
   method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json"},
   body: JSON.stringify({current_password: current, password}),
  });
  if (!response.ok) throw new Error(response.status === 401 ? "当前密码不正确" : "修改密码失败，请稍后重试");
  clearSession(); setError("密码已修改，请重新登录。");
 }, [clearSession]);

 useEffect(() => {
  const onUnauthorized = () => {clearSession();setError("登录已过期，请重新登录。");};
  window.addEventListener("tuba:unauthorized", onUnauthorized);
  return () => window.removeEventListener("tuba:unauthorized", onUnauthorized);
 }, [clearSession]);

 useEffect(() => {
  let cancelled = false;
  sessionStorage.removeItem("tuba.access_token");
  void (async () => {
   try {
    const response = await fetch("/api/v1/me", {credentials: "same-origin"});
    if (response.status === 401) return;
    if (!response.ok) throw new Error("无法读取当前用户权限");
    const value = principalSchema.parse(await response.json());
    if (!cancelled) setPrincipal(value);
   } catch (reason) {if (!cancelled) setError(reason instanceof Error ? reason.message : "读取登录状态失败");}
   finally {if (!cancelled) setLoading(false);}
  })();
  return () => {cancelled = true;};
 }, []);
 const value = useMemo<AuthValue>(() => ({principal, loading, error, login, logout, changePassword,
  can: permission => principal?.permissions.includes(permission) ?? false}), [principal, loading, error, login, logout, changePassword]);
 return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
export function useAuth(): AuthValue {
 const value = useContext(AuthContext); if (!value) throw new Error("AuthProvider missing"); return value;
}
