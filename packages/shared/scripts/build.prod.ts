import { Template } from "e2b";
import { template } from "./template.js";

async function main() {
  // default to not using cache, unless they ask for it
  const useCache = process.env.USE_CACHE === 'true';

  console.log(`cache = ${useCache ? 'enabled' : 'disabled'}`);

  await Template.build(template, {
    alias: "base",
    memoryMB: 512,
    skipCache: !useCache,
    onBuildLogs: (it) => console.log(it.toString()),
  });
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
