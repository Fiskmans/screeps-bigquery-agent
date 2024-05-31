#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const { ScreepsAPI } = require('screeps-api')
const request = require('request')
const editor = require('editor')
const args = require('commander')
const pkg = require('./package.json')
const { BQInterface } = require('./bigQueryInterface.js');

let api
let bqInterface
let setupRan = false

args
  .version(pkg.version)
  .option('-u, --username <username>', 'Private Server Username')
  .option('-p, --password <password>', 'Private Server Password')
  .option('-t, --token <token>', 'Screeps Auth Token')
  .option('--shard <shard>', 'Shard (comma seperated for multiple)')
  .option('-s, --segment <id>', 'Use Segment ID for stats')
  .option('-m, --memory', 'Use Memory for stats (default)')
  .option('-c, --console', 'Use console for stats')
  .option('-a, --sptoken <token>', 'ScreepsPl.us token')
  .option('--host <host>', 'Private Server host and port (ex: host:port)')
  .option('--https', 'Use HTTPS for Private Server')
  .option('--no-updatecheck', 'Skip check for updates')
  .option('-v, --verbose', 'Verbose')
  .parse(process.argv)

let {file, config} = loadConfig()
if (args.username || args.password || args.token || args.segment || args.memory || args.console || args.host || args.https) {
  config = config || {}
  config.screeps = config.screeps || { method: 'memory' }
  if (args.username) config.screeps.username = args.username
  if (args.password) config.screeps.password = args.password
  if (args.token) config.screeps.token = args.token
  if (args.segment) config.screeps.segment = args.segment
  if (args.memory) config.screeps.method = 'memory'
  if (args.console) config.screeps.method = 'console'
  if (args.shard) config.screeps.shard = args.shard.split(',')
  if (args.segment) {
    config.screeps.method = 'memory'
    config.screeps.segment = args.segment
  }
  if (args.host || args.https) {
    config.screeps.connect = config.screeps.connect || { protocol: 'http' }
    if (args.host) config.screeps.connect.host = args.host
    if (args.https) config.screeps.connect.protocol = args.https ? 'https' : 'http'
  }
}
if (args.sptoken) {
  config = config || {}
  config.service = config.service || { url: 'https://screepspl.us' }
  config.service.token = args.sptoken
}
if (args.verbose) {
  config.showRawStats = !!args.verbose
}
if (!config) throw new Error("missing config");

start();

async function start () {
  if (config.sampleConfig || !config.screeps) {
    console.log(file, 'does not have a valid config')
    return setup()
  }
  let ps = config.screeps.connect && config.screeps.connect.host
  api = new ScreepsAPI(config.screeps.connect)
  if (!ps && config.screeps.username) {
    console.log(`Update your config (${file}) to use auth tokens instead of username. http://blog.screeps.com/2017/12/auth-tokens/`)
    console.log(`ex: {`)
    console.log(`       token: "yourToken"`)
    console.log(`    }`)
    process.exit()
  }
  if (ps) {
    try {
      await api.auth(config.screeps.username, config.screeps.password)
    } catch (e) {
      console.log(`Authentication failed for user ${config.screeps.username} on ${api.opts.url}`)
      console.log('Check your config.js and try again')
      process.exit()
    }
  } else {
    api.token = config.screeps.token
  }
	
  bqInterface = new BQInterface(config.bigquery);

  for(const shard of config.screeps.shard)
  {
	beginMemoryStats(shard)
  }
}

function beginConsoleStats () {
  api.socket.connect()
  api.socket.on('connected', () => {
    api.socket.subscribe('console')
  })
  api.socket.on('console', (event) => {
    console.log(event)
    if (event.data.messages && event.data.messages.log) {
      event.data.messages.log
        .filter(l => l.startsWith('STATS'))
        .forEach(log => processStats(log.slice(6).replace(/;/g, '\n')))
    }
  })
}

function formatStats (data) {
  if (data[0] === '{') data = JSON.parse(data)
  if (typeof data !== 'object') throw new Error("stats misformed");

  return {
    type: 'application/json',
    stats: data
  }
}

function beginMemoryStats (shard) {
  tick(shard)
  setInterval(() => { tick(shard) }, config.interval || 60000)
}

function tick (shard) {
  Promise.resolve()
    .then(() => console.log('Fetching Stats (' + shard + ')'))
    .then(() => { return getStats(shard) })
    .then(formatStats)
	.then(pushStats)
    .catch(err => console.error(err))
}

function getStats (shard) {
  if (config.screeps.segment !== undefined) {
    return api.memory.segment.get(config.screeps.segment, shard || 'shard0').then(r => r.data)
  } else {
    return api.memory.get('stats', shard || 'shard0').then(r => r.data)
  }
}

async function pushStats (data) {
  let {type, stats} = data
  if (!stats) return console.log('No stats found, is Memory.stats defined?')
  await bqInterface.WriteToDatabase(config.bigquery, stats);
}

function getConfigPaths () {
  let appname = 'screepsplus-agent'
  let paths = []
  if (process.env.AGENT_CONFIG_PATH) { paths.push(process.env.AGENT_CONFIG_PATH) }
  paths.push(path.join(__dirname, 'config.js'))
  let create = ''
  if (process.platform == 'linux' || process.platform == 'darwin') {
    create = `${process.env.HOME}/.${appname}`
    paths.push(create)
    paths.push(`/etc/${appname}/config.js`)
  }
  if (process.platform == 'win32') {
    let dir = path.join(process.env.APPDATA, appname)
    try { fs.mkdirSync(dir) } catch (e) {}
    if (!fs.existsSync(path.join(dir, 'config.js'))) {
      fs.writeFileSync(path.join(dir, 'config.js'), fs.readFileSync(path.join(__dirname, 'config.js.sample')))
    }
    paths.push(path.join(dir, 'config.js'))
  }
  create = ''
  return { paths, create }
}

function loadConfig () {
  let {paths} = getConfigPaths()
  for (let i in paths) {
    let file = paths[i]
    try {
      console.log('Try', file)
      let config = require(file)
      console.log(file)
      return { config, file }
    } catch (e) {}
  }
  return false
}
