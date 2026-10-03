import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// Optional shared development gateway; keep the local stack as the default.
const developmentGateway = loadEnv("development", ".", "TUBA_").TUBA_DEV_GATEWAY;

export default defineConfig({
  plugins: [react(), ...(developmentGateway ? [{
    name: "tuba-development-gateway",
    configureServer(server: import("vite").ViteDevServer) {
      server.middlewares.use((request, response, next) => {
        if ((request as {url?: string}).url?.split("?")[0] !== "/config.js") return next();
        response.setHeader("Content-Type", "application/javascript");
        response.setHeader("Cache-Control", "no-store");
        response.end('window.TUBA_CONFIG = {oidcIssuer:"/tuba-auth/realms/tuba",oidcClientId:"tuba-web",basePath:"/"};');
      });
    },
  }] : [])],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: developmentGateway ? {
      "/api": {target: developmentGateway, changeOrigin: true},
      "/tuba-auth": {
        target: developmentGateway,
        changeOrigin: true,
        // Keycloak validates Origin independently of the proxy Host header.
        headers: {Origin: developmentGateway.replace(/\/$/, "")},
      },
    } : {
      "/api/v1/ingest": {target: "http://127.0.0.1:8080", changeOrigin: true},
      "/api": {target: "http://127.0.0.1:8788", changeOrigin: true},
    },
  },
  build: {outDir: "dist", sourcemap: false},
});
