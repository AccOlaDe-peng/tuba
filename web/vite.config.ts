import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api/v1/ingest": {target: "http://127.0.0.1:8080", changeOrigin: true},
      "/api": {target: "http://127.0.0.1:8788", changeOrigin: true},
    },
  },
  build: {outDir: "dist", sourcemap: false},
});
