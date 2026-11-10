// pm2 ecosystem for funbot (polybot's layout). App names carry a "fun-" prefix: pm2 names are global, polybot owns
// webui / pairv4 / pairv4-live.
//   pm2 start ecosystem.config.js --only fun-fairv1        # dry run
//   pm2 start ecosystem.config.js --only fun-fairv1-live   # live (needs FAIRV1_LIVE = True or the env below; never alongside fun-fairv1)
// A -live app writes the same tree as its dry-run app (LOG_DIR/<strategy>: positions.json, CSVs): never run both.
// DRY_RUN in the process environment wins over .env (core/config.py: load_dotenv() does not override it).
const path = require("path");
const cwd = __dirname;
const python = path.join(cwd, "venv/bin/python");
const logs = path.join(cwd, "output/pm2");

const bot = (strategy, live = false) => ({
  name: `fun-${strategy}${live ? "-live" : ""}`,
  cwd,
  script: "run.py",
  interpreter: python,
  // live: <STRATEGY>_LIVE=true in the process environment lifts the dry run guard of config/strategy/<strategy>.py for this app only
  env: { DRY_RUN: live ? "false" : "true", STRATEGY: strategy, PYTHONUNBUFFERED: "1",
         ...(live ? { [`${strategy.toUpperCase()}_LIVE`]: "true" } : {}) },
  out_file: path.join(logs, `${strategy}${live ? "-live" : ""}.out.log`),
  error_file: path.join(logs, `${strategy}${live ? "-live" : ""}.err.log`),
  time: true,
  autorestart: true,
  restart_delay: 5000,
  kill_timeout: 10000,          // SIGINT lets the bot cancel its open orders and save positions.json first
  kill_signal: "SIGINT",
});

module.exports = {
  apps: [
    bot("fairv1"),
    bot("fairv1", true),
  ],
};
