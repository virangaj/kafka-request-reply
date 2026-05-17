import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "nestjs/index": "src/nestjs/index.ts",
  },
  format: ["cjs", "esm"],
  dts: false,        // tsc handles declarations separately via build script
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  external: [
    "@nestjs/common",
    "@nestjs/core",
    "class-transformer",
    "class-validator",
  ],
});