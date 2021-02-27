module.exports = {
  apps: [
    {
      name: "nexus-bitmex-node-1",
      cmd: "startup.py",
      autorestart: true,
      env: {
        PORT: 8001,
        APP_ENV: "staging"
      },
      interpreter: "python3"
    },
    {
      name: "nexus-bitmex-node-2",
      cmd: "startup.py",
      autorestart: true,
      env: {
        PORT: 8002,
        APP_ENV: "staging"
      },
      interpreter: "python3"
    },
    {
      name: "nexus-bitmex-node-3",
      cmd: "startup.py",
      autorestart: true,
      env: {
        PORT: 8003,
        APP_ENV: "staging"
      },
      interpreter: "python3"
    },
    {
      name: "nexus-bitmex-node-4",
      cmd: "startup.py",
      autorestart: true,
      env: {
        PORT: 8004,
        APP_ENV: "staging"
      },
      interpreter: "python3"
    },
    {
      name: "nexus-bitmex-node-5",
      cmd: "startup.py",
      autorestart: true,
      env: {
        PORT: 8005,
        APP_ENV: "staging"
      },
      interpreter: "python3"
    },
  ]
};