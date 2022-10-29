#!/usr/bin/env node

import fs from 'fs';
import nsblob from 'nsblob';

import { saturate, store } from '.';

async function main() {
	if (fs.existsSync(process.argv[2])) {
		await store(fs.createReadStream(process.argv[2])).then(console.log);
	} else if (process.argv[2]) {
		await saturate(process.argv[2], process.stdout);
	} else {
		await store(process.stdin).then(console.log);
	}

	nsblob.socket.close();
}

main();
