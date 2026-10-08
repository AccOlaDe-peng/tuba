import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// Optional shared development gateway; keep the local stack as the default.
const developmentGateway = loadEnv("development", ".", "TUBA_").TUBA_DEV_GATEWAY;

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: developmentGateway ? {
      "/api": {target: developmentGateway, changeOrigin: true},
    } : {
      "/api/v1/ingest": {target: "http://127.0.0.1:8080", changeOrigin: true},
      "/api": {target: "http://127.0.0.1:8788", changeOrigin: true},
    },
  },
  build: {outDir: "dist", sourcemap: false},
});
