/*
 * Pulsar
 * Copyright (C) 2013-2015 eBay Software Foundation
 * Licensed under the GPL v2 license.  See LICENSE for full terms.
 */
'use strict';

app.controller('batchCtrl',function($scope, $rootScope, MetricService, $q){
	
	$rootScope.pageOrSession = $rootScope.pageOrSession || 'Page';
	$rootScope.countryFilter = $rootScope.countryFilter || undefined;
	
	var pageOrSessionSwitch = jQuery('#pageOrSessionSwitch').find('label');
	for(var i = 0; i < pageOrSessionSwitch.size(); i++){
		var label = pageOrSessionSwitch.eq(i);
		if( label.text().indexOf( $rootScope.pageOrSession ) != -1){
			label.addClass('active');
		} else {
			label.removeClass('active');
		}
	};

	//overall pageview
	function getOverallPageviews(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname=pageviews"},{},function(data){
			var values = [];
			data.forEach(function(i){
				var dp = {
						x	:	new Date(i.timestamp),
						y	:	i.value
				};
				values.push(dp);
			});
			var trendChartData = [{
					key		:	'Overall Page Views',
					values	:	values
			}];
			defer.resolve( trendChartData );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//drill-downed pageview
	function getDrillDownedPageviews(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cn"},{},function(data){
			var base = crossfilter(data);
			//var data = base.dimension(function(fact){return fact.groupId;}).filter("Germany").top(Infinity);
			var data = base.dimension(function(fact){return fact.groupId;}).filter($rootScope.countryFilter).top(Infinity);
			var values = [];
			data.forEach(function(i){
				var dp = {
						x	:	new Date(i.timestamp),
						y	:	i.value
				};
				values.push(dp);
			});
			values.sort(function(d1,d2){return d1.x - d2.x; });
			var trendChartData = [{
					key		:	'DrillDowned Page Views',
					values	:	values
			}];
			defer.resolve( trendChartData );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//pageview wrapper for mixing overall and drill-downed
	function getPageviewsWrapper(){
		( $rootScope.countryFilter == undefined ? getOverallPageviews() : getDrillDownedPageviews() ).then(function(trendChartData){
			$scope.trendChartData = trendChartData;
		},function(){
			$scope.trendChartData = [];
		});
	};
	
	//overall session
	function getOverallSession(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname=visitors"},{},function(data){
			var values = [];
			data.forEach(function(i){
				var dp = {
						x	:	new Date(i.timestamp),
						y	:	i.value
				};
				values.push(dp);
			});
			var trendChartData = [{
					key		:	'Overall Session',
					values	:	values
			}];
			defer.resolve( trendChartData );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//drill-downed session
	function getDrillDownedSession(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname=vistspercn"},{},function(data){
			var base = crossfilter(data);
			// var data = base.dimension(function(fact){return fact.groupId;}).filter("Germany").top(Infinity);
			var data = base.dimension(function(fact){return fact.groupId;}).filter($rootScope.countryFilter).top(Infinity);
			var values = [];
			data.forEach(function(i){
				var dp = {
						x	:	new Date(i.timestamp),
						y	:	i.value
				};
				values.push(dp);
			});
			values.sort(function(d1,d2){return d1.x - d2.x; });
			var trendChartData = [{
					key		:	'DrillDowned Session',
					values	:	values
			}];
			defer.resolve( trendChartData );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//session wrapper for mixing overall and drill-downed
	function getSessionWrapper(){
		var promise = ($rootScope.countryFilter == undefined ? getOverallSession() : getDrillDownedSession());
		promise.then(function(trendChartData){
			$scope.trendChartData = trendChartData;
		},function(){
			$scope.trendChartData = [];
		});
	};
	
	//overall country
	function getOverallCountry(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cn"},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.groupId;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function( item ){
				return {
					name	:	item.key,
					value	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//drill-downed country
	function getDrillDownedCountry(){
		var defer = $q.defer();
		// MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandcity&groupid=Germany"},{},function(data){
		MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandcity&groupid="+$rootScope.countryFilter},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.tagMap.tag_value;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function(item){
				return {
					// name	:	'Germany',
					name	:	item.key,
					value	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//country wrapper for mixing overall and drill-downed
	function getCountryWrapper(){
		($rootScope.countryFilter == undefined ? getOverallCountry() : getDrillDownedCountry() ).then(function(countries){
			$scope.tableData = countries;
		},function(){
			$scope.tableData = [];
		});
	};
	
	//overall os
	function getOverallOs(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"os"},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.groupId;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function( item ){
				return {
					key	:	item.key,
					val	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//drill-downed os
	function getDrillDownedOs(){
		var defer = $q.defer();
		// MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandos&groupid=Germany"},{},function(data){
		MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandos&groupid="+$rootScope.countryFilter},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.tagMap.tag_value;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function( item ){
				return {
					key	:	item.key,
					val	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//os wrapper for mixing overall and drill-downed
	function getOsWrapper(){
		( $rootScope.countryFilter== undefined?  getOverallOs() : getDrillDownedOs() ).then(function(osData){
			$scope.osChartData = osData;
		},function(){
			$scope.osChartData = [];
		});
	};
	
	//overall browser
	function getOverallBrowsers(){
		var defer = $q.defer();
		MetricService.query({params:"columnFamilyName=mc_groupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"bf"},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.groupId;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function( item ){
				return {
					key	:	item.key,
					val	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//drill-downed browser
	function getDrillDownedBrowsers(){
		var defer = $q.defer();
		// MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandbf&groupid=Germany"},{},function(data){
		MetricService.query({params:"columnFamilyName=mc_countrygroupmetric&metricname="+($rootScope.pageOrSession == 'Page' ? 'pgsper' :'vistsper')+"cnandbf&groupid="+$rootScope.countryFilter},{},function(data){
			var base = crossfilter(data);
			var result = base.dimension(function(fact){return fact.tagMap.tag_value;}).group().reduceSum(function(fact){return fact.value;}).top(Infinity);
			var transformed = result.map(function( item ){
				return {
					key	:	item.key,
					val	:	item.value
				};
			});
			defer.resolve( transformed );
		}, function(data){
			defer.reject();
		});
		return defer.promise;
	};
	//browser wrapper for mixing overall and drill-downed
	function getBrowsersWrapper(){
		( $rootScope.countryFilter == undefined ? getOverallBrowsers() : getDrillDownedBrowsers() ).then(function( browserChartData ){
			$scope.browserChartData = browserChartData;
		},function(){
			$scope.browserChartData = [];
		});
	};
	
	//global refresh function
	function refresh(){
		$scope.renderLinearChart($rootScope.pageOrSession);
		getCountryWrapper();
		getOsWrapper();
		getBrowsersWrapper();
	};
	
	//take prepared stuff into effect
	$scope.renderLinearChart = function(pageOrSession){
		if (pageOrSession == 'Page' ){
			$rootScope.pageOrSession = 'Page';
			getPageviewsWrapper();
			getCountryWrapper();
			getOsWrapper();
			getBrowsersWrapper();
		} else{
			$rootScope.pageOrSession = 'Session';
			getSessionWrapper();
			getCountryWrapper();
			getOsWrapper();
			getBrowsersWrapper();
		};
	};
	$scope.dirlldown = function( country ){
		$rootScope.countryFilter = country;
		refresh();
	};
	$scope.removeCountryFilter = function(){
		$rootScope.countryFilter = undefined;
		refresh();
	};
	
	refresh();
});