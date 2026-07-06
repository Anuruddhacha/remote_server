function timestamp() {
  return new Date().toISOString();
}

function writeLine(level, step, message, data) {
  let line = `[${timestamp()}] [${step}]`;
  if (level === 'WARN') {
    line += ' WARN';
  } else if (level === 'ERROR') {
    line += ' ERROR';
  }
  line += `: ${message}`;
  if (data !== undefined) {
    line += ` ${JSON.stringify(data)}`;
  }
  // Back4App / Docker capture stderr more reliably than stdout
  process.stderr.write(line + '\n');
}

function log(step, message, data) {
  writeLine('INFO', step, message, data);
}

function warn(step, message, data) {
  writeLine('WARN', step, message, data);
}

function error(step, message, data) {
  writeLine('ERROR', step, message, data);
}

module.exports = { log, warn, error };
