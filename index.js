// write our server
// read from redis channel
// update ui in real time
// - get command line arguments

// - get arguments
var argv = require('minimist')(process.argv.slice(2)); // process is a hidden variable, the running program is a process
var port = argv['port'];
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];
var subscribe_channel = argv['subscribe_channel'];
var subscribe_tweet_channel = argv['subscribe_tweet_channel'];
var subscribe_trend_channel = argv['subscribe_trend_channel'];

// - setup dependency instances
var express = require('express');
var app = express();
var server = require('http').createServer(app);
// - open a session instead of just send request
var io = require('socket.io')(server);

// - setup redis client
var redis = require('redis');
console.log('Creating a redis client');
var redisclient = redis.createClient(redis_port, redis_host);

console.log('Subscribing to redis topic %s', subscribe_tweet_channel);
redisclient.subscribe(subscribe_tweet_channel);
console.log('Subscribing to redis topic %s', subscribe_channel);
redisclient.subscribe(subscribe_channel);
console.log('Subscribing to redis topic %s', subscribe_trend_channel);
redisclient.subscribe(subscribe_trend_channel);
// - call back function
redisclient.on('message', function (channel, message) {
	//console.log('channel is %s', channel)
    if (channel == subscribe_channel) {
        //console.log('message received %s', message);
        io.sockets.emit('data', message);
    }
    else if (channel == subscribe_tweet_channel) {
    	//console.log("tweet message received %s", message);
    	io.sockets.emit("tweet", message)
    }
    else if (channel == subscribe_trend_channel) {
    	//console.log("trend message received %s", message);
    	io.sockets.emit("trend", message)
    }
});

// - setup webapp routing
// - path
app.use(express.static(__dirname + '/public'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));

server.listen(port, function () {
    console.log('Server started at port %d.', port);
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting redis client');
    redisclient.quit();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);