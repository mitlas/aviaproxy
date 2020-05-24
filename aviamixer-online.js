#!/usr/local/bin/node



/*---- config ----*/

var cfg_host = '127.0.0.1'; // listen on localhost address by default
var cfg_port = 8889; // listen to 8888 port by default

var cfg_timeout = 110; // timeout in seconds before output available data

var cfg_max_timeouts_percent = 7; // max timeout percent in served requests before controlled restart
var cfg_max_timeouts = 5; // max served requests before controlled restart
var cfg_max_requests = 300; // max served requests before controlled restart

var cfg_services = // available service providers (add the new one here & then update code "send request to")
[
 {
  name: 'svyaznoy',
  url: 'https://secure.onetwotrip.com/_api/searching/startSync/',
  //url: 'https://www.svyaznoy.travel/o_api/searching/startSync/',
  urlParams: 'urlParams'
 },
 {
  name: 'svyaznoy_online_test',
  url: 'http://127.0.0.1/o_api/',
  urlParams: 'urlParams.replace(\'svyaznoy\', \'svyaznoy_online_test\') + \'func=searching/startSync/\''
 }
];



/*---- debug ----*/
var debug_numRequests = 0, debug_numTimeouts = 0; // log requests & timeouts

var memwatch = require('memwatch');
memwatch.on
(
 'leak',
 function (info)
 {
  console.error('---------------------------------> POSSIBLE MEMORY LEAK: ' + info.reason);
 }
);



/*---- code ----*/
var os = require('os'); // system info, etc.
var cluster = require('cluster'); // let's create cluster by number of CPUs/cores

if (cluster.isMaster) // create cluster, fork workers
{
 console.log
 (
  'AviaMixer 1.0.8. Service provider proxy/multiplexor.' + os.EOL +
  'Copyright (c) 2013 Svyaznoy.Travel. All rights reserved.' + os.EOL +
  'Usage: ' + process.argv[1].substr((process.argv[1].lastIndexOf('/') > -1) ? // discover current name of the script
   (process.argv[1].lastIndexOf('/') + 1) : process.argv[1]) + ' [-address IP] [-port NUM]' + os.EOL
 );

 var numCPUs = os.cpus().length - 2; // limit num of workers by num of CPUs minus 2
 numCPUs = ((numCPUs > 0) ? numCPUs : 1);

 for (var i = 0; i < numCPUs; ++i)
  cluster.fork();

 cluster.on
 (
  'listening',
  function (worker, address)
  {
   console.log('worker ' + worker.process.pid + ' is online on ' + address.address + ":" + address.port);
  }
 ).on
 (
  'exit',
  function (worker, code, signal)
  {
   if (signal)
   {
    console.error('worker ' + worker.process.pid + ' killed by signal ' + signal + ', restarting...');
    cluster.fork();
   }
   else
    if(code == 0)
     console.log('worker ' + worker.process.pid + ' exited normally (code ' + worker.process.exitCode + ').');
    else
     if (code == 2)
      console.log('Address ' + cfg_host + ':' + cfg_port + ' is in use - check for running copies of mixer.');
     else
     {
      console.error('worker ' + worker.process.pid + ' exited with error code ' + code + ', restarting...');
      cluster.fork();
     }
  }
 );
}
else // run the worker process
{
 var fs = require('fs');
 
 var http = require('http'); http.globalAgent.maxSockets = 9999;
 //var https = require('https'); https.globalAgent.maxSockets = 9999;
 var request = require('request');

 var options = // HTTPS options
 {
  key: fs.readFileSync('/usr/home/svyaznoy/sites/www.svyaznoy.travel/certs/server.key'),
  cert: fs.readFileSync('/usr/home/svyaznoy/sites/www.svyaznoy.travel/certs/cert.pem')
 };

 process.argv.forEach // read arguments from command line if specified
 (
  function (val, index, array)
  {
   if (((val == '-address') || (val == '--address') || (val == '-a')) && ((index + 1) < process.argv.length))
    cfg_host = process.argv[index + 1];

   if (((val == '-port') || (val == '--port') || (val == '-p')) && ((index + 1) < process.argv.length))
    cfg_port = process.argv[index + 1];
  }
 );

 //var server = https.createServer // create HTTPS server
 var server = http.createServer // create HTTP server
 (
  /*options,*/
  function (req, res)
  {
   var debug_overall_time = (new Date()).getTime();
   console.log('INCOMING REQUEST @ ' + Math.round(debug_overall_time / 1000));

   var routes = []; // all received routes

   var urlParams = ''; // collect variables from request object
   var query = require('url').parse(req.url, true).query;

   for (var i in query)
    urlParams += i + '=' + query[i] + '&';

   for (var i in cfg_services)
   {
    request.post // send request to service
    (
     {
      //url: cfg_services[i].url,
      url: cfg_services[i].url + '?' + cfg_services[i].name + '=' + (new Date()).getTime(),
      headers:
      {
       'connection': 'keep-alive',
       'content-type': 'application/x-www-form-urlencoded',
       'charset': 'utf-8'
      },
      pool: { maxSockets: 9999 },
      body: eval(cfg_services[i].urlParams)
     },
     function (err, res, body)
     {
      if ((err != undefined) || (res.statusCode != 200) || (body == undefined) || (body == ''))
      {
       console.log('+++++++++++++++++++++++++++++++++ EMPTY BODY OR ERROR +++++++++++++++++++++++++' + os.EOL + 'error:[' + err + ']');
       console.dir(res);
       body = {};
      }
      else
       try // protection from broken JSON
       {
        body = JSON.parse(body);
       }
       catch (e) // in a case of raw "504 Gateway Time-out" HTML response, etc.
       {
        console.log('++++++++++++++++++++++++++++++++++ UNEXPECTED BODY +++++++++++++++++++++++++++');
        console.dir(res);
        body = {};
       }

      routes[routes.length] =
      {
       id: ((body.vp_enable != undefined) ? 1 : 0), // detect vipservice (temporal workaround)
       data: body,
       name: (((body.gdsInf != undefined && body.gdsInf.name != undefined) ? body.gdsInf.name : ((body.vp_enable != undefined) ? 'vipservice' : 'empty data'))),
       time: (((new Date()).getTime() - parseInt(res.request.path.substr(res.request.path.indexOf('=') + 1))) / 1000)
      };

      console.log('receive data <- (' + (((new Date()).getTime() - parseInt(res.request.path.substr(res.request.path.indexOf('=') + 1))) / 1000) +
       ' secs) ' + ((err != undefined) ? err : (((body.gdsInf != undefined && body.gdsInf.name != undefined) ?
       body.gdsInf.name : ((body.vp_enable != undefined) ? 'vipservice' : 'empty data') ))));

      if (routes.length >= cfg_services.length) // all data received
       sendResponse(false);
     }
    );

    console.log('send request -> ' + cfg_services[i].name + ' (' + eval(cfg_services[i].urlParams) + ')');
   }

   var timeout = setTimeout // in case of timeout from any service
   (
    function () { sendResponse(true); },
    cfg_timeout * 1000
   );

/*
   function logTimeouts()
   {
    console.log('============================= TIMEOUT INFO ' + process.pid + ' (' + (Math.round(debug_numTimeouts * 100 / debug_numRequests * 100) / 100) + '% of ' + debug_numRequests + ' rqs)');

    setTimeout
    (
     function () { logTimeouts(); },
     30 * 1000
    );    
   }

   logTimeouts();
*/

   // output collected routes
   function sendResponse(isTimeout)
   {
    clearTimeout(timeout); // prevent call by timeout

    // ---- start debug code
    var debug = 'sendResponse() call on ----> ';

    ++debug_numRequests;

    if (isTimeout)
    {
     ++debug_numTimeouts;
     debug += 'TIMEOUT (' + (Math.round(debug_numTimeouts * 100 / debug_numRequests * 100) / 100) + '% of ' + debug_numRequests + ' rqs)';
    }
    else
     debug += 'data';

    console.log(debug);
    // ---- finish debug code

    var hashes = [];
    var maxHashSize = 0;
    var maxHashIndex = 0;

    for (var i = 0; i < routes.length; ++i) // build hashes & find hash with max amount of routes
    {
     var frs = routes[i].data.frs;

     if (frs != undefined)
     {
      var curHashSize = 0;

      for (var f in frs)
      {
       var hash = 'h'; // otherwise hash can begin from a digit & stale JS engine

       for (var d in frs[f].dirs)
       {
        hash += frs[f].dirs[d].jrnTm;

        for (var t in frs[f].dirs[d].trps)
        {
         hash += frs[f].dirs[d].trps[t].cls + frs[f].dirs[d].trps[t].srvCls;

         var trip = routes[i].data.trps[frs[f].dirs[d].trps[t].id];

         hash += trip.stDt + trip.stTm + trip.endTm + trip.from + trip.to + trip.airCmp + trip.fltNm + trip.fltTm + trip.plane;
        }
       }

       frs[f].aviamixer_hash = hash;
       frs[f].source = cfg_services[routes[i].id].name;

       if (hashes[i] == undefined)
        hashes[i] = [];

       hashes[i][hash] = { id: f, price: Math.round(frs[f].prcInf.amt * ((frs[f].prcInf.cur == 'RUB') ? 1 : routes[i].data.rates[frs[f].prcInf.cur + 'RUB']) * 100) / 100 };
       ++curHashSize;
     
       delete hash;
      }

      if (curHashSize > maxHashSize)
      {
       maxHashSize = curHashSize;
       maxHashIndex = i;
      }
     }

     delete frs;
    }



    if (hashes.length > 1)
    {
     var copiedRoutes = 0, addedRoutes = 0;

     for (var i in routes[maxHashIndex].data.frs) // discover best price, copy best route to largest array & 
     {
      var curHash = routes[maxHashIndex].data.frs[i].aviamixer_hash;
      var curPrice = hashes[maxHashIndex][curHash].price;

      for (var j in hashes)
      {
       if (j == maxHashIndex)
        continue;

       if (hashes[j][curHash] && (hashes[j][curHash].price < curPrice))
       {
        curPrice = hashes[j][curHash].price;

        routes[maxHashIndex].data.frs[i] = routes[j].data.frs[hashes[j][curHash].id];
  
        for (var d in routes[maxHashIndex].data.frs[i].dirs) // collect trips
         for (var t in routes[maxHashIndex].data.frs[i].dirs[d].trps)
          routes[maxHashIndex].data.trps[routes[maxHashIndex].data.frs[i].dirs[d].trps[t].id] =
           routes[j].data.trps[routes[maxHashIndex].data.frs[i].dirs[d].trps[t].id];

        for (var p in routes[j].data.planes)
         routes[maxHashIndex].data.planes[p] = routes[j].data.planes[p];

        ++copiedRoutes;
       }

       delete hashes[j][curHash];
      }
     }



     if (routes[maxHashIndex].data.trps.length != undefined) // reconstruct trps as Object, not Array (trps ids can be hash)
     {
      var trps = {};
      
      for (var i in routes[maxHashIndex].data.trps)
       trps['' + i] = routes[maxHashIndex].data.trps[i];
      
      delete routes[maxHashIndex].data.trps;
      routes[maxHashIndex].data.trps = trps;
      delete trps;
     }



     for (var j in hashes) // copy routes left in hashes
     {
      if (j == maxHashIndex)
       continue;

      if (routes[j].data.vp_enable != undefined) // temporal workaround
       routes[maxHashIndex].data.vp_enable = routes[j].data.vp_enable;

      if (routes[j].data.rates['RUBEUR'] != undefined) // temporal workaround
       routes[maxHashIndex].data.rates = routes[j].data.rates;

      if (routes[j].data.gdsInfs.length > 1) // temporal workaround
       routes[maxHashIndex].data.gdsInfs = routes[j].data.gdsInfs;

      for (var h in hashes[j])
      {
       var frsIdx = routes[maxHashIndex].data.frs.length;
   
       routes[maxHashIndex].data.frs[frsIdx] = routes[j].data.frs[hashes[j][h].id];

       for (var d in routes[j].data.frs[hashes[j][h].id].dirs) // collect trips
        for (var t in routes[j].data.frs[hashes[j][h].id].dirs[d].trps)
         routes[maxHashIndex].data.trps[routes[j].data.frs[hashes[j][h].id].dirs[d].trps[t].id] =
          routes[j].data.trps[routes[j].data.frs[hashes[j][h].id].dirs[d].trps[t].id];

       for (var p in routes[j].data.planes)
        routes[maxHashIndex].data.planes[p] = routes[j].data.planes[p];

       ++addedRoutes;
      }
     }
    }



    // add timings
    routes[maxHashIndex].data.timings = {};
    for (var i in routes)
     routes[maxHashIndex].data.timings[routes[i].name] = routes[i].time;
    routes[maxHashIndex].data.timings.timeSpent = (((new Date()).getTime() - debug_overall_time) / 1000);



    // send data
    res.writeHead(200, {'Content-Type': 'application/json'});

    if (hashes.length == 0)
     res.end('{"vp_enable":"nodata","frs":[]}'); // empty fares or no data from all services due to timeout
    else
     res.end(JSON.stringify(routes[maxHashIndex].data));

    delete routes;
    delete hashes;



    console.log('copied routes (better price): ' + copiedRoutes + ', added routes (unique): ' + addedRoutes + ', time spent (overall): ' +
     (((new Date()).getTime() - debug_overall_time) / 1000) + ' secs' + os.EOL + os.EOL + os.EOL);

    // end worker if percent of timeouts is too high
    if
    (
     (debug_numTimeouts >= cfg_max_timeouts) ||
     ((Math.round(debug_numTimeouts * 100 / debug_numRequests * 100) / 100) > cfg_max_timeouts_percent) ||
     (debug_numRequests > cfg_max_requests)
    )
    {
     console.error('=======================> CONTROLLED RESTART <=========== ' +
      (Math.round(debug_numTimeouts * 100 / debug_numRequests * 100) / 100) +
      '% of ' + debug_numRequests + ' rqs ====================================');
     process.exit(8888);
    }



   } // end of sendResponse()
  } // end of POST callback
 ).listen(cfg_port, cfg_host);
 
 server.timeout = 180000;

 server.on // report errors as 'port is busy', etc.
 (
  'error',
  function(error)
  {
   process.exit(2);
  }
 );

}
