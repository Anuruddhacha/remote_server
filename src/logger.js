function timestamp() {
  return new Date().toISOString();
}

function log(step, message, data) {
  const prefix = `[${timestamp()}] [${step}]`;
  if (data !== undefined) {
    console.log(prefix, message, data);
  } else {
    console.log(prefix, message);
  }
}

function warn(step, message, data) {
  const prefix = `[${timestamp()}] [${step}] WARN:`;
  if (data !== undefined) {
    console.warn(prefix, message, data);
  } else {
    console.warn(prefix, message);
  }
}

function error(step, message, data) {
  const prefix = `[${timestamp()}] [${step}] ERROR:`;
  if (data !== undefined) {
    console.error(prefix, message, data);
  } else {
    console.error(prefix, message);
  }
}

module.exports = { log, warn, error };
