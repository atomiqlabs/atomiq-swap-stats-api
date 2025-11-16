const {container} = require("../libs/db");

const currencyDataFor = process.env.ASSETS.split(",");
const chainDataFor = process.env.CHAINS.split(",");

const CACHE_TIMEOUT = 60*1000;

let cache = null;

const useTimeframes = [
    {name: "24h", duration: 24*60*60},
    {name: "7d", duration: 7*24*60*60},
    {name: "30d", duration: 30*24*60*60}
];

async function refreshCache(context) {

    let totalUsdVolume;
    let totalSwapCount;

    let totalQueryCharge = 0;

    {
        const {resources, requestCharge} = await container.items.query("SELECT SUM(c._usdValue) as volume, COUNT(1) as count FROM c WHERE c.success").fetchAll();
        totalUsdVolume = Math.round(resources[0].volume*100)/100;
        totalSwapCount = resources[0].count;
        totalQueryCharge += requestCharge;
    }

    const now = Math.floor(Date.now()/1000);
    const timeframes = {};
    for(let {name, duration} of useTimeframes) {
        const {resources, requestCharge} = await container.items.query("SELECT SUM(c._usdValue) as volume, COUNT(1) as count FROM c WHERE c.success AND c.timestampInit>"+(now-duration)).fetchAll();
        timeframes[name] = {
            count: resources[0].count,
            volumeUsd: Math.round(resources[0].volume*100)/100
        };
        totalQueryCharge += requestCharge;
    }

    const currencyData = {};
    for(let currency of currencyDataFor) {
        const {resources, requestCharge} = await container.items.query("SELECT COUNT(1) AS count, SUM(c._tokenAmount) AS volume, SUM(c._usdValue) AS volumeUsd FROM c WHERE c.success AND c.tokenName=\""+currency+"\"").fetchAll();
        currencyData[currency] = {
            count: resources[0].count,
            volume: resources[0].volume,
            volumeUsd: Math.round(resources[0].volumeUsd*100)/100
        };
        totalQueryCharge += requestCharge;
    }

    const chainData = {};
    for(let chain of chainDataFor) {
        const {resources, requestCharge} = await container.items.query("SELECT COUNT(1) AS count, SUM(c._usdValue) AS volumeUsd FROM c WHERE c.success AND c.chainId=\""+chain+"\"").fetchAll();
        chainData[chain] = {
            count: resources[0].count,
            volumeUsd: Math.round(resources[0].volumeUsd*100)/100
        };
        totalQueryCharge += requestCharge;
    }

    context.log("Request query charge: ", totalQueryCharge);

    cache = {
        data: {
            totalSwapCount,
            totalUsdVolume,
            currencyData,
            chainData,
            timeframes
        },
        timestamp: Date.now()
    };

}

module.exports = async function (context, req) {

    if(cache==null || cache.timestamp+CACHE_TIMEOUT<=Date.now()) await refreshCache(context);

    context.res = {
        status: 200,
        body: cache.data
    };
    context.done();

};