import fs from 'fs';
import http from 'http';
import nsblob from 'nsblob';
import { PassThrough, Readable, Writable } from 'stream';

export async function store(
	stream: fs.ReadStream | http.IncomingMessage | Readable
): Promise<string> {
	const promises = new Array<Promise<string>>();

	let buffer = Buffer.alloc(0);

	stream.on('data', (chunk: string | Buffer) => {
		buffer = Buffer.concat([buffer, Buffer.from(chunk)]);

		while (buffer.length >= 0x10000) {
			promises.push(nsblob.store(buffer.subarray(0, 0x10000)));
			buffer = buffer.subarray(0x10000);
		}
	});

	await new Promise((resolve) => stream.on('end', resolve));

	if (buffer.length) {
		promises.push(nsblob.store(buffer));
	}

	const hashes = await Promise.all(promises);

	return await nsblob.store(
		Buffer.concat(hashes.map((hash) => Buffer.from(hash, 'hex')))
	);
}

export async function saturate(
	hash: string,
	stream: fs.WriteStream | http.ServerResponse | Writable
) {
	const hashes = (await nsblob.fetch(hash)).toString('hex');

	for (let i = 0; i < hashes.length; i += 64) {
		stream.write(await nsblob.fetch(hashes.slice(i, i + 64)));
	}

	stream.end();
}

export function fetch(hash: string): PassThrough {
	const stream = new PassThrough();

	saturate(hash, stream);

	return stream;
}
