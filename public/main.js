$(function () {

    var data_points = [];

    var tweets = [];

    var stock_symbols = {};
    var stock_trends = {};
    var size = 0;

    $("#chart").height($(window).height() - $("#header").height() * 2);
    $("#chart").width(650);
    //$("svg").css({top: 0, left: 400, right:800, position:'absolute'});


    $(document.body).on('click', '.stock-label', function () {
        "use strict";
        var symbol = $(this).text();
        $.ajax({
            url: 'http://localhost:5000/' + symbol,
            type: 'DELETE'
        });

        $(this).remove();
        if (stock_symbols.hasOwnProperty(symbol)) {
            var i = getSymbolIndex(symbol, data_points);
            data_points.splice(i, 1);
            console.log(data_points);
            delete stock_symbols[symbol]
        }
        
    });

    $("#add-stock-button").click(function () {
        "use strict";
        var symbol = $("#stock-symbol").val();

        $.ajax({
            url: 'http://localhost:5000/' + symbol,
            type: 'POST'
        });

        $("#stock-symbol").val("");
        if (!stock_symbols.hasOwnProperty(symbol)) {
            data_points.push({
                values: [],
                key: symbol
            });
            stock_symbols[symbol] = symbol
        }
        

        $("#stock-list").append(
            "<a class='stock-label list-group-item small'>" + symbol + "</a>"
        );

        console.log(data_points);
    });

    function getSymbolIndex(symbol, array) {
        "use strict";
        for (var i = 0; i < array.length; i++) {
            if (array[i].key == symbol) {
                return i;
            }
        }
        return -1;
    }

    var chart = nv.models.lineChart()
        .interpolate('monotone')
        .margin({
            bottom: 100
        })
        .useInteractiveGuideline(true)
        .showLegend(true)
        .color(d3.scale.category10().range());

    chart.xAxis
        .axisLabel('Time')
        .tickFormat(formatDateTick);

    chart.yAxis
        .axisLabel('Price');

    nv.addGraph(loadGraph);

    function loadGraph() {
        "use strict";
        d3.select('#chart svg')
            .datum(data_points)
            .transition()
            .duration(5)
            .call(chart);

        nv.utils.windowResize(chart.update);
        return chart;
    }

    function newDataCallback(message) {
        "use strict";
        var parsed = JSON.parse(message);
        var timestamp = parsed['timestamp'];
        var average = parsed['average'];
        var symbol = parsed['symbol'];
        var point = {};
        point.x = timestamp;
        point.y = average;

        console.log(point);

        var i = getSymbolIndex(symbol, data_points);

        data_points[i].values.push(point);
        if (data_points[i].values.length > 100) {
            data_points[i].values.shift();
        }
        loadGraph();
    }

    function newTrendCallback(trend) {
        "use strict";
        var parsed = JSON.parse(trend);
        var trend  = parsed['trend'];
        var stock_price_difference = parsed['stock_price_difference'];
        var symbol = parsed['symbol'];
        var trade_time = parsed['trade_time'];
        var time_elapsed = parsed['time_elapsed'];
        console.log("begin to add trend row")
        if (time_elapsed > 0) {
            if (stock_trends.hasOwnProperty(symbol)) {
                document.getElementById("stock-trend-table").rows[stock_trends[symbol]].cells.namedItem(symbol + "_trade_time").innerHTML = trade_time;
                document.getElementById("stock-trend-table").rows[stock_trends[symbol]].cells.namedItem(symbol + "_price_change").innerHTML = stock_price_difference;
                document.getElementById("stock-trend-table").rows[stock_trends[symbol]].cells.namedItem(symbol + "_trend").innerHTML = trend;
                document.getElementById("stock-trend-table").rows[stock_trends[symbol]].cells.namedItem(symbol + "_time_elapsed").innerHTML = time_elapsed;
            }
            else {
                stock_trends[symbol] = size;
                size++;
                $("#stock-trend").append(
                    "<tr>"
                    + "<td class='col-lg-2' style='font-size: 50%' id=" + symbol + " >" + symbol + "</td>"
                    + "<td class='col-lg-2' style='font-size: 50%' id=" + symbol + "_trade_time >" + trade_time + "</td>"
                    + "<td class='col-lg-2' style='font-size: 50%' id=" + symbol + "_price_change >" + stock_price_difference + "</td>"
                    + "<td class='col-lg-2' style='font-size: 50%' id=" + symbol + "_trend >" + trend + "</td>"
                    + "<td class='col-lg-2' style='font-size: 50%' id=" + symbol + "_time_elapsed >" + time_elapsed + "</td>"
                    + "</tr>"
                    );
            }
        }
    }

    function newTweetCallback(tweet) {
        "use strict";
        var result = tweet.split("^$$^");
        var symbol = result[0];
        var user_id = result[1];
        var user_name = result[2];
        var tweet_created_at = result[3];
        var text = result[4];
        console.log("begin to add table row");
        $("#stock-tweet").append(
            "<tr>" 
            + "<td class='col-lg-2' style='font-size: 50%'> " + symbol + "</td>" 
            + "<td class='col-lg-2' style='font-size: 50%'> " + user_id + "</td>" 
            + "<td class='col-lg-2' style='font-size: 50%'> " + user_name + "</td>" 
            + "<td class='col-lg-2' style='font-size: 50%'> " + tweet_created_at + "</td>" 
            + "<td class='col-lg-10' style='font-size: 50%'> " + text + "</td>" 
            + "</tr>"
            );
    }

    function formatDateTick(time) {
        "use strict";
        var date = new Date(time * 1000);
        return d3.time.format('%H:%M:%S')(date);
    }

    var socket = io();

    socket.on('tweet', function (tweet) {
        //console.log('received tweet' + tweet);
        newTweetCallback(tweet);
    });

    socket.on('trend', function (trend) {
        //console.log('received stock trend ' + trend);
        newTrendCallback(trend);
    });

    // - Whenever the server emits 'data', update the flow graph
    socket.on('data', function (data) {
        newDataCallback(data);
    });

});