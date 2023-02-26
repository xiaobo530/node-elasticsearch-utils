import pMap from 'p-map';
import got from 'got';

const sites = [
	// getWebsiteFromUsername('sindresorhus'), //=> Promise
	'https://www.baidu.com',
	'https://news.sohu.com'
];

const mapper = async (site) => {
	const {requestUrl} = await got.head(site);
	return requestUrl;
};

const result = await pMap(sites, mapper, {concurrency: 2});

console.log(result);