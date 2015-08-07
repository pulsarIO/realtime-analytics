/*
 * Pulsar
 * Copyright (C) 2013-2015 eBay Software Foundation
 * Licensed under the GPL v2 license.  See LICENSE for full terms.
 */
'use strict';

app.controller('twitterFeedCtrl',function($scope, $rootScope, $timeout){
    
    var trendDatas = [],
        tableDatas = [],
        countryDatas = [],
        languageDatas = [];

    $scope.initTwitterFeedWebsocket = function(){
        console.info('init twitter feed websocket');
        $scope.websocketTwitterFeed = $rootScope.pulsarmetric('TwitterEventCount&TwitterTopHashTagCount&TwitterTopLangCount&TwitterTopCountryCount', 'ABC', $scope.twitterFeedRenderData);
    }   

    $scope.twitterFeedcountdown = function(){
        if($scope.twitterFeedCounter > 1){
            $scope.twitterFeedCounter--;
            $scope.twitterFeedcountdownPromise = $timeout($scope.twitterFeedcountdown,1000);
        }
    };

    $scope.twitterFeedRenderData = function(datas){
        var objs = JSON.parse(datas);

        var dataType = '';
        
        if (objs && objs.length >0){
            dataType = objs[0].js_ev_type;
            if (dataType == 'TwitterEventCount'){
                trendDatas = [];
                 if ($scope.initTwitterFeedCounter){
                    if($scope.twitterFeedcountdownPromise){
                        $timeout.cancel($scope.twitterFeedcountdownPromise);
                    }
                    console.info('reset twitter feed counter');
                    $scope.twitterFeedCounter = 10;
                    $scope.twitterFeedcountdownPromise = $timeout($scope.twitterFeedcountdown,1000);
                } else {
                    $scope.initTwitterFeedCounter = true;
                }
                
            } else if(dataType == 'TwitterTopHashTagCount') {
                tableDatas = [];
            } else if (dataType == 'TwitterTopLangCount') {
                languageDatas = [];
            } else if  (dataType == 'TwitterTopCountryCount') {
                countryDatas = [];
            }
        }

        //Adjust data
        objs.forEach(function(el){
            
            if (dataType == 'TwitterEventCount'){
                // trend chart
                var trendData = {x:el.timestamp, y:el.value};
                trendDatas.push(trendData);
            } else if(dataType == 'TwitterTopHashTagCount') {
                // table
                var hashTag = el.TopHashTags;
                for (var property in hashTag) {
                    if (hashTag.hasOwnProperty(property)) {
                        var tableData = {hashtag:property, value:hashTag[property]};
                        tableDatas.push(tableData);
                    }
                }
            } else if (dataType == 'TwitterTopLangCount') {
                // browser chart
                var languageData = {key:el.lang, val:el.value};
                languageDatas.push(languageData);
            } else if  (dataType == 'TwitterTopCountryCount') {
                // OS chart
                var countryData = {key:el.country, val:el.value};
                countryDatas.push(countryData);
            }
        });

        $scope.$apply(function () {
            if (dataType == 'TwitterEventCount'){
               $scope.trendChartData = [{key:'Events',values:trendDatas}];
            } else if(dataType == 'TwitterTopHashTagCount') {
                tableDatas.sort(function (a, b) {
                    return (+a.value) < (+b.value);
                });
                $scope.tableData = tableDatas;
            } else if (dataType == 'TwitterTopLangCount') {
                $scope.languageChartData = languageDatas;
            } else if  (dataType == 'TwitterTopCountryCount') {
                $scope.countryChartData = countryDatas;
            }
        });
    }

    angular.element(document).ready(function () {
        $scope.initTwitterFeedCounter = false;
        $scope.twitterFeedCounter = 10;
        $scope.initTwitterFeedWebsocket();
    });
    
    
    $scope.$on('$destroy',function(){
        $scope.websocketTwitterFeed.connection.onclose = function () {
            console.info('close twitter feed websocket connection');
            if($scope.twitterFeedcountdownPromise)
                $timeout.cancel($scope.twitterFeedcountdownPromise);
            $scope.initTwitterFeedCounter = false;
        };
        $scope.websocketTwitterFeed.connection.close();
    });

});
