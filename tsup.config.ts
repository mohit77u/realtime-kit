import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    'server/index': 'src/server/index.ts',
    'client/index': 'src/client/index.ts',
    'react/index': 'src/react/index.ts',
    'vue/index': 'src/vue/index.ts',
    'angular/index': 'src/angular/index.ts',
    'nest/index': 'src/nest/index.ts',
    'redis/index': 'src/redis/index.ts',
  },
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  minify: false,
  treeshake: true,
  target: 'es2020',
  external: ['ws', 'react', 'vue', 'rxjs', '@nestjs/common', '@nestjs/core', 'reflect-metadata', 'ioredis'],
  outExtension: ({ format }) => ({ js: format === 'cjs' ? '.cjs' : '.js' }),
});
