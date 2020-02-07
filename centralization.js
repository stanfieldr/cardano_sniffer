const fs       = require('fs');
const moment   = require('moment');
const process  = require('process');
const readline = require('readline');
const request  = require('request-promise');

const EXPLORER_URL = "http://localhost:3100/explorer/graphql";

const lineReader = readline.createInterface({
    input: fs.createReadStream('stake_pool_log.txt')
});

// Key = Block hash, Value = [{ node_id, announcment_time }, ...]
let block_announce_history = new Map();

// Key = Node ID, Value = ["IP", ...]
let node_ips = new Map();

// Key = IP, Value = ["Node ID", ...]
let ip_nodes = new Map();

// [{known_ips: ["IP"], known_ids: ["Node ID"]}]
let players = [];

// ["Pool ID"]
let unique_pools = new Set();
let solved_pools = new Set();

function addNodeIP(node_id, peer_addr) {
    if (!node_ips.has(node_id)) {
        node_ips.set(node_id, new Set());
    }

    node_ips.get(node_id).add(peer_addr);
}

function addIPNode(node_id, peer_addr) {
    if (!ip_nodes.has(peer_addr)) {
        ip_nodes.set(peer_addr, new Set());
    }

    ip_nodes.get(peer_addr).add(node_id);
}

function logPlayer(player, ip, ids) {
    if (player) {
        let has_ip = player.known_ips.includes(ip);

        if (!has_ip) {
            player.known_ips.push(ip);
        } else {
            ids.forEach(id => player.known_ids.add(id));
        }
    } else {
        players.push({
            known_ids: new Set(ids),
            known_ips: [ip],
            block_announcements: []
        });
    }
}

function mergePlayers() {

    let acc = {};
let c = 0;
    players.forEach(player => {
        if (!player.pool_id) {
            return;
        }

        console.log('Pool ID: ', player.pool_id);
        if (acc[player.pool_id]) {
            player.known_ids.forEach(id => acc[player.pool_id].known_ids.add(id));
            acc[player.pool_id].known_ips = player.known_ips.concat(acc[player.pool_id].known_ips);
            acc[player.pool_id].block_announcements = player.block_announcements.concat(acc[player.pool_id].block_announcements);
            return;
        }

        acc[player.pool_id] = player;
        c++;
    });

    console.log("C: ", c);
    players = Object.values(acc);
}

function stitchCommonPlayers() {
    for (let [ip, ids] of ip_nodes) {
        let id_arr = Array.from(ids);
        let player_aliases = players.filter(p => p.known_ips.includes(ip) || id_arr.some(id => p.known_ids.has(id)));

        if (player_aliases.length <= 1) {
            let player = player_aliases.length ? player_aliases[0] : null;
            logPlayer(player, ip, id_arr);
        }
    }
}

function findPlayerBlockAnnouncements() {
    for (let [block_hash, announcements] of block_announce_history) {
        announcements =announcements.map(announcement => {
            return {
                node_id: announcement.node_id,
                time   : Number(moment.utc(announcement.time, "MMM DD HH:mm:ss.SSS").format('X'))
            };
        });

        announcements.sort((a, b) => {
            if (a.time === b.time) {
                return 0;
            }

            return a.time > b.time ? 1 : -1;
        });

        announcements.forEach((announcement, index) => {
            let player = players.find(p => p.known_ids.has(announcement.node_id));

            if (player === undefined) {
                player = {
                    known_ids          : new Set([ announcement.node_id ]),
                    known_ips          : [],
                    block_announcements: []
                };

                players.push(player);
            }

            player.block_announcements.push({
                hash : block_hash,
                order: index
            });
        });
    }
}

function associateAnnouncementToPool() {
    var options = {
        method: 'POST',
        uri: EXPLORER_URL,
        body: {
            query: `{
                allStakePools{
                    edges{
                        node{
                            id,
                            blocks {
                                edges {
                                    node {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }`
        },
        json: true
    };

    return request(options).then(response => {
        response.data.allStakePools.edges.forEach(pool_edge => {
            let pool         = pool_edge.node;
            let block_hashes = pool.blocks.edges.map(block_edge => block_edge.node.id);

            block_hashes.forEach(block_hash => {
                players
                    .filter(p => p.block_announcements.some(a => a.hash === block_hash))
                    .forEach(p => {
                        let announcements = p.block_announcements.filter(a => a.hash === block_hash);

                        announcements.forEach(a => a.pool_id = pool.id);
                    });
            });
        });
    });
}

function associatePoolToPlayer() {
    players.forEach(player => {
        let pool_tally    = {};
        let pool_distance = {};
        let pool_ranks    = [];

        player.block_announcements.forEach(announcement => {
            if (announcement.pool_id) {
                if (!pool_tally.hasOwnProperty(announcement.pool_id)) {
                    pool_tally[announcement.pool_id] = 0;
                    pool_distance[announcement.pool_id] = 0;
                }

                pool_tally[announcement.pool_id]++;
                pool_distance[announcement.pool_id] += announcement.order;
            }
        });

        if (Object.keys(pool_tally).length) {
            for (let pool_id in pool_tally) {
                pool_ranks.push({
                    pool_id,
                    rank: pool_distance[pool_id] / pool_tally[pool_id]
                });
            }

            player.potential_pool_ids = pool_ranks.filter(pr => pr.rank === 0);
            player.potential_pool_ids.forEach(pool_id => {
                unique_pools.add(pool_id);
            });
        }
    });

    // Process of elimination
    let progress;
    do {
        progress = false;

        players.forEach(player => {
            if (player.pool_id || !player.potential_pool_ids) {
                return;
            }

            if (player.potential_pool_ids.length === 1) {
                player.pool_id = player.potential_pool_ids[0].pool_id;
                solved_pools.add(player.pool_id);
                progress = true;
                return;
            }

            let could_be = player.potential_pool_ids.filter(pid => !solved_pools.has(pid));
            if (could_be.length === 1) {
                player.pool_id = could_be[0].pool_id;
                solved_pools.add(player.pool_id);
                progress = true;
                return;
            }
        });
    } while (progress);
}

lineReader.on('line', line => {
    let is_block_announcement = line.indexOf('announcement') > 0;
    let has_ip_node           = line.indexOf('peer_addr') > 0 && line.indexOf('node_id') > 0;

    if (has_ip_node) {
        let node_id   = line.substring(line.indexOf('node_id:') + 9);
        let peer_addr = line.substring(line.indexOf('peer_addr:') + 11);

        node_id   = node_id.substring(0, node_id.indexOf(',')).trim();
        peer_addr = peer_addr.substring(0, peer_addr.indexOf(',')).trim();
        peer_addr = peer_addr.split(':')[0];

        addNodeIP(node_id, peer_addr);
        addIPNode(node_id, peer_addr);

    } else if (is_block_announcement) {
        let time       = line.substring(0, line.indexOf('INFO') - 1);
        let node_id    = line.substring(line.indexOf('node_id:') + 9);
        let block_hash = line.substring(line.indexOf('hash:') + 6);

        node_id    = node_id.substring(0, node_id.indexOf(',')).trim();
        block_hash = block_hash.substring(0, block_hash.indexOf(','));

        if (!block_announce_history.has(block_hash)) {
            block_announce_history.set(block_hash, []);
        }

        block_announce_history.get(block_hash).push({
            node_id,
            time
        });
    }
});

lineReader.on('close', () => {
    let playerCount = 0;

    // Keep trying to stitch till it can't find anything in common
    do {
        playerCount = players.length;
        stitchCommonPlayers();
    } while (playerCount !== players.length);

    findPlayerBlockAnnouncements();
    associateAnnouncementToPool().then(() => {
        associatePoolToPlayer();
        mergePlayers();

        console.log('Player Count: ', players.length);
    });

});