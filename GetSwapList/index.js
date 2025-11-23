const {container} = require("../libs/db");

const currencyDataFor = process.env.ASSETS.split(",");
const excludeTokens = process.env.EXCLUDE_ASSETS?.split(",") ?? [];

const allowedChains = process.env.CHAINS.split(",");

const HEX_REGEX = /[0-9a-f]+/i;

/*
Query options:
    - chainId?
    - limit?: default 50, max 100, min 10
    - endTime?

    - token?
    - chain?
    - search?

    - clientAddress?
    - lpAddress?
    - btcTxId?
    - txId?

    - startTs?
    - endTs?
 */

module.exports = async function (context, req) {

    const queryParams = [];
    const values = [];

    if(req.query.endTime!=null) {
        const endTimeParsed = parseInt(req.query.endTime);
        if(
            endTimeParsed==null ||
            isNaN(endTimeParsed)
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        queryParams.push("c.timestampInit < @endTime");
        values.push({
            name: "@endTime",
            value: endTimeParsed
        });
    }

    if(req.query.startTs!=null) {
        const startTsParsed = parseInt(req.query.startTs);
        if(
            startTsParsed==null ||
            isNaN(startTsParsed)
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param (startTs)"
            };
            return;
        }

        queryParams.push("c._ts > @startTs");
        values.push({
            name: "@startTs",
            value: startTsParsed
        });
    }

    if(req.query.endTs!=null) {
        const endTsParsed = parseInt(req.query.endTs);
        if(
            endTsParsed==null ||
            isNaN(endTsParsed)
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param (endTs)"
            };
            return;
        }

        queryParams.push("c._ts < @endTs");
        values.push({
            name: "@endTs",
            value: endTsParsed
        });
    }

    let limit = 50;
    if(req.query.limit!=null) {
        const limitParsed = parseInt(req.query.limit);
        if(
            limit==null ||
            isNaN(limit) ||
            limit<10 ||
            limit>100
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }
        limit = limitParsed;
    }

    let pointer = 0;
    for(let ignoreToken of excludeTokens) {
        queryParams.push("c.tokenName != @excludeToken"+pointer);
        values.push({
            name: "@excludeToken"+pointer,
            value: ignoreToken
        });
    }

    if(req.query.token!=null) {
        const tokens = Array.isArray(req.query.token)
            ? req.query.token
            : [req.query.token];
        const tokenQueryParams = [];
        for(let i=0;i<tokens.length;i++) {
            const token = tokens[i];
            if(currencyDataFor.includes(token)) {
                context.res = {
                    status: 400,
                    body: "Invalid query param (token)"
                };
                return;
            }

            tokenQueryParams.push("c.tokenName = @tokenName"+i);
            values.push({
                name: "@tokenName"+i,
                value: token
            });
        }

        if(tokenQueryParams.length>0) queryParams.push("("+tokenQueryParams.join(" OR ")+")");
    }

    if(req.query.clientAddress!=null) {
        if(
            req.query.clientAddress.length===0
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        queryParams.push("c.clientWallet = @clientAddress");
        values.push({
            name: "@clientAddress",
            value: req.query.clientAddress
        });
    }

    if(req.query.chain!=null) {
        const chains = Array.isArray(req.query.chain)
            ? req.query.chain
            : [req.query.chain];

        const chainQueryParams = [];

        for(let i=0;i<chains.length;i++) {
            const chain = chains[i];

            if(chain==="BITCOIN") {
                chainQueryParams.push("c.type = \"CHAIN\"");
            } else if(chain==="LIGHTNING") {
                chainQueryParams.push("c.type = \"LN\"");
            } else if(chain==="SOLANA") {
                chainQueryParams.push("(c.chainId = \"SOLANA\" OR NOT isDefined(c.chainId))");
            } else {
                chainQueryParams.push("c.chainId = @chainId");
                values.push({
                    name: "@chain"+i,
                    value: chain
                });
            }
        }

        if(chainQueryParams.length>0) queryParams.push("("+chainQueryParams.join(" OR ")+")");
    }

    if(req.query.chainId!=null) {
        if(
            typeof(req.query.chainId)!=="string" ||
            !allowedChains.includes(req.query.chainId)
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        if(req.query.chainId==="SOLANA") {
            queryParams.push("(c.chainId = @chainId OR NOT isDefined(c.chainId))");
        } else {
            queryParams.push("c.chainId = @chainId");
        }
        values.push({
            name: "@chainIds",
            value: req.query.chainId
        });
    }

    if(req.query.lpAddress!=null) {
        if(
            req.query.lpAddress.length===0
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        queryParams.push("c.lpWallet = @lpAddress");
        values.push({
            name: "@lpAddress",
            value: req.query.lpAddress
        });
    }

    if(req.query.btcTxId!=null) {
        if(
            req.query.btcTxId.length!==64 ||
            !HEX_REGEX.test(req.query.btcTxId)
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        queryParams.push("c.btcTx = @btcTxId");
        values.push({
            name: "@btcTxId",
            value: req.query.btcTxId
        });
    }

    if(req.query.search!=null) {
        if(
            req.query.search.length===0
        ) {
            context.res = {
                status: 400,
                body: "Invalid query param"
            };
            return;
        }

        if(HEX_REGEX.test(req.query.search) && req.query.search.length===64) {
            //BTC tx id or payment hash
            queryParams.push("(c.paymentHash = @search OR c.btcTx = @search)");
            values.push({
                name: "@search",
                value: req.query.search
            });
        } else {
            queryParams.push("(c.txInit = @search OR c.txFinish = @search OR c.lpWallet = @search OR c.clientWallet = @search OR c.btcAddress = @search OR ARRAY_CONTAINS(c.btcInAddresses, @search))");
            values.push({
                name: "@search",
                value: req.query.search
            });
        }
    }

    let querySpec = "SELECT * FROM c";
    if(queryParams.length>0) {
        querySpec += " WHERE "+queryParams.join(" AND ");
    }
    querySpec += " ORDER BY c.timestampInit DESC";

    const query = container.items.query({
        query: querySpec,
        parameters: values
    }, {
        maxItemCount: limit+1
    });

    const resp = await query.fetchNext();

    console.log("Query charge: ", resp.requestCharge);

    let last = true;
    if(resp.resources.length===limit+1) {
        last = false;
        resp.resources.pop();
    }

    resp.resources.forEach(e => {
        e.chainId ??= "SOLANA";
        delete e._rid;
        delete e._self;
        delete e._etag;
        delete e._attachments;
    });

    context.res = {
        status: 200,
        body: {
            data: resp.resources,
            last,
            endTime: resp.resources[resp.resources.length-1].timestampInit
        }
    };

};