import * as fsp from 'node:fs/promises';
import * as fs from 'node:fs';
import * as workerThreads from 'node:worker_threads';
import LineByLine from 'n-readlines'
const MAX_LINE_LENGTH = 65000
let MAX_MEM_USAGE = 0
const logMemUs = () => {
  MAX_MEM_USAGE = Math.max(process.memoryUsage().heapTotal / 1024 / 1024)
}
async function main(filename: string) {
  if (workerThreads.isMainThread) {
    const file = await fsp.open(filename);
    const size = (await file.stat()).size;
    const chunkSize = Math.floor(MAX_LINE_LENGTH);
    const chunkOffsets = [];
    let offset = 0;
    const bufFindNl = Buffer.alloc(MAX_LINE_LENGTH);
    while (true) {
      offset += chunkSize;
      if (offset >= size) {
        chunkOffsets.push(size);
        break;
      }
      await file.read(bufFindNl, 0, MAX_LINE_LENGTH, offset);
      const nlPos = bufFindNl.indexOf(10); // EOL
      bufFindNl.fill(0);
      if (nlPos === -1) {
        chunkOffsets.push(size);
        break;
      } else {
        offset += nlPos + 1;
        chunkOffsets.push(offset);
      }
    }

    await file.close();
    let stoppedWorkers = 0;
    for (let i = 0; i < chunkOffsets.length; i++) {
      const worker = new workerThreads.Worker('./index.ts', {
        workerData: {
          index: i,
          filename: filename,
          start: i === 0 ? 0 : chunkOffsets[i - 1],
          end: chunkOffsets[i],
        },
      });
      worker.on(
        'message',
        (
          message
        ) => {
          console.log(message)
        }
      );
      worker.on('error', (err) => {
        console.error(err);
      });
      worker.on('exit', async (code) => {
        if (code !== 0) {
          new Error(`Worker stopped with exit code ${code}`);
        } else {
          console.log('Worker stopped');
        }
        stoppedWorkers++;

        if (stoppedWorkers === chunkOffsets.length) {
          for (let i = 1; i < chunkOffsets.length; i *= 2) {
            const filenames = fs.readdirSync("./sorted")
            if (filenames.length < 2) {
              return;
            }
            const queue = [...filenames]
            while (queue.length > 1) {
              const f1 = queue.pop()
              const f2 = queue.pop()
              if (!!f1 && !!f2) {
                mergeFiles(f1, f2, filenames.length)
                fs.unlinkSync(`./sorted/${f1}`)
                fs.unlinkSync(`./sorted/${f2}`)
              }
            }
          }
          console.log("MEMUSAGE", MAX_MEM_USAGE, "MB")
        }
      });
    }
    console.log(chunkOffsets)
  } else {
    const {index, filename, start, end} = workerThreads.workerData;
    console.log(`worker ${index} received`, start, end)

    if (start > end - 1) {
      workerThreads?.parentPort?.postMessage?.(new Map());
    } else {
      const readStream = fs.createReadStream(filename, {
        start: start,
        end: end - 1,
      });
      parseStream(index, readStream);
    }
  }
}
function parseStream(index: number, readStream: fs.ReadStream) {
  readStream.on("data", (chunk: Buffer) => {
    const lines: string[] = []
    let currentLine = ""
    for (let [_, el] of chunk.entries()) {
      if (el === 10) {
        lines.push(currentLine)
        currentLine = "";
      } else {
        currentLine += String.fromCharCode(el)
      }
    }
    lines.push(currentLine)
    const data = lines
      .filter(el => !!el)
      .sort()
    fs.writeFileSync(`./sorted/${index}`, data.join('\n'))
  })
  readStream.on('end', () => {
    workerThreads?.parentPort?.postMessage?.(new Map());
  });
}
function mergeFiles(file1: string, file2: string, offset: number) {
  const r1 = new LineByLine(`./sorted/${file1}`)
  const r2 = new LineByLine(`./sorted/${file2}`)
  const filename = `./sorted/${offset + Math.max(+file1, +file2)}`
  fs.writeFileSync(filename, "")
  let v1 = r1.next();
  let v2 = r2.next();

  while (v1 || v2) {
    if (v1 && v2) {
      if (v1.compare(v2) < 0) {
        fs.appendFileSync(filename, v1)
        fs.appendFileSync(filename, '\n')
        v1 = r1.next()
      } else {
        fs.appendFileSync(filename, v2)
        fs.appendFileSync(filename, '\n')
        v2 = r2.next()
      }
    } else if (!v1) {
      while (v2) {
        fs.appendFileSync(filename, v2)
        fs.appendFileSync(filename, '\n')
        v2 = r2.next()
      }
      break;
    } else {
      while (v1) {
        fs.appendFileSync(filename, v1)
        fs.appendFileSync(filename, '\n')
        v1 = r1.next()
      }
      break;
    }
  }
}

main("./file").then(() => {
  console.log('done')
})