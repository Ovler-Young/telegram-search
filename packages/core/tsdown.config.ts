import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/services/etm-recovery-config.ts',
    'src/types/index.ts',
  ],
  dts: true,
  sourcemap: true,
  unused: true,
  fixedExtension: true,
})
