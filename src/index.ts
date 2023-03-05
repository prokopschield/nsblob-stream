import fs from 'fs';
import http from 'http';
import nsblob from 'nsblob';
import { PassThrough, Readable, Writable } from 'stream';

const CHUNK_LENGTH = 0x10000;

/**
 * Store a stream in the nodesite cdn
 * @param stream the stream you wish to store
 * @param properties an object which will be updated as the stream is read
 * @returns a promise of the stream's hash
 */
export async function store(
	stream: fs.ReadStream | http.IncomingMessage | Readable,
	properties: {
		chunks?: number;
		done?: boolean;
		length?: number;
		uploaded?: number;
	} = {}
): Promise<string> {
	let chunks = 0;
	let length = 0;
	let uploaded = 0;
	const promises = new Array<Promise<string>>();

	let buffer = Buffer.alloc(0);

	stream.on('data', (chunk: string | Buffer) => {
		properties.done = false;
		properties.chunks = chunks += 1;
		properties.length = length += chunk.length;
		buffer = Buffer.concat([buffer, Buffer.from(chunk)]);

		while (buffer.length >= CHUNK_LENGTH) {
			promises.push(nsblob.store(buffer.subarray(0, CHUNK_LENGTH)));
			buffer = buffer.subarray(CHUNK_LENGTH);
		}
	});

	stream.resume();

	await new Promise<void>((resolve, reject) => {
		let resolved = false;

		stream.on('end', () => {
			if (!resolved) {
				resolved = true;
				resolve();
			}
		});

		stream.on('error', (error) => {
			if (!resolved) {
				resolved = true;
				reject(error);
			}
		});
	});

	if (buffer.length) {
		properties.chunks = chunks += 1;
		promises.push(nsblob.store(buffer));
	}

	properties.done = true;

	for (const promise of promises) {
		await promise;

		properties.uploaded = uploaded += CHUNK_LENGTH;
	}

	properties.uploaded = uploaded = length;

	const hashes = await Promise.all(promises);

	return await nsblob.store(
		Buffer.concat(hashes.map((hash) => Buffer.from(hash, 'hex')))
	);
}

export async function saturate(
	hash: string,
	stream: fs.WriteStream | http.ServerResponse | Writable,
	startAt: number = 0,
	stopAt: number = Number.MAX_SAFE_INTEGER
) {
	let streamEnded = false;
	let cb = () => {};

	stream.on('close', () => ((streamEnded = true), cb()));
	stream.on('error', () => ((streamEnded = true), cb()));
	stream.on('finish', () => ((streamEnded = true), cb()));

	stopAt -= startAt;

	const hashes = (await nsblob.fetch(hash))
		.toString('hex')
		.slice((startAt >> 16) << 6);

	startAt %= CHUNK_LENGTH;

	for (let i = 0; i < hashes.length; i += 64) {
		let buffer = await nsblob.fetch(hashes.slice(i, i + 64));
		const sub = Math.min(buffer.length, startAt);

		if (sub) {
			startAt -= sub;
			buffer = buffer.subarray(sub);
		}

		if (buffer.length > stopAt) {
			buffer = buffer.subarray(0, stopAt);
		}

		if (streamEnded || !stopAt) {
			return stream.end();
		}

		stopAt -= buffer.length;

		if (!stream.write(buffer)) {
			await new Promise<void>((resolve) => {
				let resolved = false;

				cb = () => {
					if (!resolved) {
						resolved = true;
						resolve();
					}
				};

				stream.once('drain', cb);
			});
		}

		if (streamEnded || !stopAt) {
			return stream.end();
		}
	}

	return stream.end();
}

export function fetch(
	hash: string,
	startAt: number = 0,
	stopAt: number = Number.MAX_SAFE_INTEGER
): PassThrough {
	const stream = new PassThrough();

	saturate(hash, stream, startAt, stopAt);

	return stream;
}

export function store_buffer(buffer: Buffer) {
	const stream = new PassThrough();

	setTimeout(() => {
		stream.write(buffer);
		stream.end();
	});

	return store(stream);
}

export function fetch_buffer(
	hash: string,
	startAt: number = 0,
	stopAt: number = Number.MAX_SAFE_INTEGER
): Promise<Buffer> {
	const stream = fetch(hash, startAt, stopAt);
	const chunks = Array<Buffer>();

	stream.on('data', (chunk) => chunks.push(chunk));

	return new Promise<Buffer>((resolve) => {
		stream.on('end', () => resolve(Buffer.concat(chunks)));
	});
}

export class Source<T extends Record<string, string>> {
	private _hash: string;
	private _length = 0;
	private _ready = false;
	private _stream = '';

	constructor(hash: string) {
		if ((this._hash = hash || '')) {
			this.change(this._hash);
		}
	}

	async change(hash: string = this._hash) {
		this._ready = false;
		Object.assign(this, await nsblob.fetch_json((this._hash = hash)));
		this._ready = true;
	}

	async update(props: Record<string, number | string> = {}) {
		props = {
			...Object.fromEntries(
				[...Object.entries(this)].filter(
					([key]) => !key.startsWith('_')
				)
			),
			...props,
			_length: this._length,
			_stream: this._stream,
		};

		delete props.props;
		Object.assign(this, props);

		return (this._hash = await nsblob.store_json(props));
	}

	get hash() {
		return this._hash;
	}

	set hash(hash) {
		this.change(hash);
	}

	get length() {
		return this._length;
	}

	/** returns hash of raw stream, can be passed to non-Source methods */
	get raw() {
		return this._stream;
	}

	get props(): T {
		const self = this;
		const record = self as Record<string, unknown>;

		return new Proxy(record as T, {
			get(_target, key) {
				if (key === Symbol.toPrimitive) {
					return () => self._hash;
				}

				return String(record[String(key)] ?? '');
			},

			set(_target, key, value) {
				self.update({ [key]: (record[String(key)] = String(value)) });

				return true;
			},
		});
	}

	set props(props) {
		this.update(props);
	}

	/** clone this Source with new props */
	async clone<N extends Record<string, string>>(
		props: N
	): Promise<Source<N & T>> {
		const new_source = new Source<N & T>('');

		await new_source.change(this.hash);
		await new_source.update(props);

		return new_source;
	}

	async toBuffer(startAt: number = 0, stopAt: number = this._length) {
		if (!this._ready) {
			await this.change(this._hash);
		}

		return fetch_buffer(this._stream, startAt, stopAt);
	}

	async toStream(startAt: number = 0, stopAt: number = this._length) {
		if (!this._ready) {
			await this.change(this._hash);
		}

		return fetch(this._stream, startAt, stopAt);
	}

	async saturate(
		stream: fs.WriteStream | http.ServerResponse | Writable,
		startAt: number = 0,
		stopAt: number = Number.MAX_SAFE_INTEGER
	) {
		return saturate(this._stream, stream, startAt, stopAt);
	}

	static async fromBuffer<T extends Record<string, string>>(
		buffer: Buffer,
		props: Partial<T> = {}
	): Promise<Source<T>> {
		const source = new Source<T>('');

		source._length = buffer.length;
		source._stream = await store_buffer(buffer);

		await source.update(
			Object.fromEntries(
				[...Object.entries(props)].filter(([, value]) => value)
			)
		);

		return source;
	}

	static async fromStream<T extends Record<string, string>>(
		stream: fs.ReadStream | http.IncomingMessage | Readable,
		props: Partial<T> = {}
	) {
		const properties = { length: 0 };
		const source = new Source<T>('');

		source._stream = await store(stream, properties);
		source._length = properties.length;

		await source.update(
			Object.fromEntries(
				[...Object.entries(props)].filter(([, value]) => value)
			)
		);

		return source;
	}

	static async fromHash<T extends Record<string, string>>(hash: string) {
		const source = new Source<T>('');

		await source.change(hash);

		return source;
	}
}

export const socket: { close: Function } = nsblob.socket;
