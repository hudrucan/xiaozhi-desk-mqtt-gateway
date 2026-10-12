const fs = require('node:fs');
const PATH = '/run/desk-node-maintenance/state.json';
let stamp = 0, cached = false, expires = 0;
function requested(node) {
  const now = Date.now();
  if (now - stamp < 250) return cached && now < expires * 1000;
  stamp = now;
  let fd;
  try {
    fd = fs.openSync(PATH, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) || stat.size > 2048) throw Error();
    const raw = Buffer.alloc(2049);
    const length = fs.readSync(fd, raw, 0, raw.length, 0);
    if (length > 2048) throw Error();
    const value = JSON.parse(raw.subarray(0, length));
    expires = value.expires;
    cached = value.protocol === 'desk-node-maintenance-v1' && value.node === node &&
      value.boot_id === fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() &&
      Number.isInteger(expires) && now < expires * 1000 && expires * 1000 <= now + 660000;
  } catch { cached = false; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
  return cached;
}
module.exports = { requested };
