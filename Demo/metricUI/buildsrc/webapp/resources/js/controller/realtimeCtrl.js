/*
 * Copyright 2013-2015 eBay Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

app.controller('realtimeCtrl',function($scope, $rootScope, $timeout){
    
    var trendDatas = [],
        tableDatas = [],
        OSDatas = [],
        browserDatas = [];

    $scope.initRealTimeWebsocket = function(){
        console.info('init realtime web socket');
        $scope.realTimeWebsocketMetric = $rootScope.pulsarmetric('MC_Metric&PulsarOsCount&PulsarBrowserCount&PulsarTopCountryCount', 'ABC', $scope.realTimeRenderData);
    }

    $scope.realTimeCountdown = function(){
        if($scope.realTimeCounter > 1){
            $scope.realTimeCounter--;
            $scope.realTimeCountdownPromise = $timeout($scope.realTimeCountdown,1000);
        }
    };

    $scope.realTimeRenderData = function(datas){
        var objs = JSON.parse(datas);

        var dataType = '';
        
        if (objs && objs.length >0){
            dataType = objs[0].js_ev_type;
            if (dataType == 'MC_Metric'){
                trendDatas = [];
                if ($scope.initRealTimeCounter){
                    if($scope.realTimeCountdownPromise){
                        $timeout.cancel($scope.realTimeCountdownPromise);
                    }
                    console.info('reset realtime counter');
                    $scope.realTimeCounter = 10;
                    $scope.realTimeCountdownPromise = $timeout($scope.realTimeCountdown,1000);
                } else {
                    $scope.initRealTimeCounter = true;
                }   
            } else if(dataType == 'PulsarTopCountryCount') {
                tableDatas = [];
            } else if (dataType == 'PulsarBrowserCount') {
                browserDatas = [];
            } else if  (dataType == 'PulsarOsCount') {
                OSDatas = [];
            }
        }

        //Adjust data
        objs.forEach(function(el){
            
            if (dataType == 'MC_Metric'){
                // trend chart
                var trendData = {x:el.timestamp, y:el.value};
                trendDatas.push(trendData);
            } else if(dataType == 'PulsarTopCountryCount') {
                // table
                var tableData = {country:el.country, value:el.value};
                tableDatas.push(tableData);
            } else if (dataType == 'PulsarBrowserCount') {
                // browser chart
                var browserData = {key:el.browser, val:el.value};
                browserDatas.push(browserData);
            } else if  (dataType == 'PulsarOsCount') {
                // OS chart
                var OSData = {key:el.os, val:el.value};
                OSDatas.push(OSData);
            }
        });

        $scope.$apply(function () {
            if (dataType == 'MC_Metric'){
               $scope.trendChartData = [{key:'Metrics',values:trendDatas}];
            } else if(dataType == 'PulsarTopCountryCount') {
                tableDatas.sort(function (a, b) {
                    return (+a.value) < (+b.value);
                });
                $scope.tableData = tableDatas;
            } else if (dataType == 'PulsarBrowserCount') {
                $scope.browserChartData = browserDatas;
            } else if  (dataType == 'PulsarOsCount') {
                $scope.osChartData = OSDatas;
            }
        });
    }

    angular.element(document).ready(function () {
        $scope.initRealTimeCounter = false;
        $scope.realTimeCounter = 10;
        $scope.initRealTimeWebsocket();
    });
    
    
    $scope.$on('$destroy',function(){
        $scope.realTimeWebsocketMetric.connection.onclose = function () {
            console.info('close realtime websocket connection');
            if($scope.realTimeCountdownPromise)
                $timeout.cancel($scope.realTimeCountdownPromise);
            $scope.initRealTimeCounter = false;
        };
        $scope.realTimeWebsocketMetric.connection.close();
    });

});
