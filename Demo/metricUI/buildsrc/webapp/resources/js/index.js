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
var app = angular.module("indexApp", ['ngResource','ng-nvd3']).run(function($rootScope,$timeout){

    // trend chart x axis format
    $rootScope.xTimeFormat = function(data){
        return moment(data).format('H:mm:ss');
    }

    // switch page
    $rootScope.pageUrl = 'views/realtime.html';
    
    $rootScope.switchPage = function(pageURL){
        $rootScope.pageUrl = 'views/'+pageURL+'.html';
    }

    $rootScope.gotoMCEPL = function(){
        window.open('http://'+location.host+'/pulsar/metriccalculator?$format=xml');
    }

    // web socket generator
    $rootScope.pulsarmetric = function() {
        
        var pulsarmetric = {
            connect : connect,
            connection: connection
        };

        var connection = null;

        function connect(source, query, callback) {
            if (connection != null)
                connection.close();
            
            if (!window.MozWebSocket && !window.WebSocket) {
                alert('Please use Firefox or Chrome.');
                return null;
            }
            
            if(query == null || query.length == 0)
                query = '&';
            
            query = query.replace(/=/g, 'equal');

            var WS = window.MozWebSocket ? MozWebSocket : WebSocket;
            pulsarmetric.connection = new WS(
                    'ws://' + location.host + '/websocket',
                    [source, query]);

            pulsarmetric.connection.onmessage = function(evt) {
                if(callback != null)
                    callback(evt.data);
            };
            return pulsarmetric;
        }

        return arguments.length
            ? connect(arguments[0], arguments[1], arguments[2])
            : pulsarmetric;
    }

}).directive('ngBanner', function() {
    return function(scope, elem, attrs) {
        attrs.$observe('ngBanner', function(banner) {
            var mapping = [{'Andorra':'flag flag-ad'},{'United Arab Emirates':'flag flag-ae'},{'Afghanistan':'flag flag-af'},{'Antigua And Barbuda':'flag flag-ag'},{'Anguilla':'flag flag-ai'},{'Albania':'flag flag-al'},{'Armenia':'flag flag-am'},{'Netherlands Antilles':'flag flag-an'},{'Angola':'flag flag-ao'},{'Argentina':'flag flag-ar'},{'American Samoa':'flag flag-as'},{'Austria':'flag flag-at'},{'Australia':'flag flag-au'},{'Aruba':'flag flag-aw'},{'Azerbaijan':'flag flag-az'},{'Bosnia And Herzegowina':'flag flag-ba'},{'Barbados':'flag flag-bb'},{'Bangladesh':'flag flag-bd'},{'Belgium':'flag flag-be'},{'Burkina Faso':'flag flag-bf'},{'Bulgaria':'flag flag-bg'},{'Bahrain':'flag flag-bh'},{'Burundi':'flag flag-bi'},{'Benin':'flag flag-bj'},{'Bermuda':'flag flag-bm'},{'Brunei Darussalam':'flag flag-bn'},{'Bolivia':'flag flag-bo'},{'Brazil':'flag flag-br'},{'Bahamas':'flag flag-bs'},{'Bhutan':'flag flag-bt'},{'Bouvet Island (Norway)聽':'flag flag-bv'},{'Botswana':'flag flag-bw'},{'Belarus':'flag flag-by'},{'Belize':'flag flag-bz'},{'Canada':'flag flag-ca'},{'Catalonia':'flag flag-catalonia'},{'Congo, The Drc':'flag flag-cd'},{'Central African Republic':'flag flag-cf'},{'Congo':'flag flag-cg'},{'Switzerland':'flag flag-ch'},{'Cote D\'Ivoire':'flag flag-ci'},{'Cook Islands':'flag flag-ck'},{'Chile':'flag flag-cl'},{'Cameroon':'flag flag-cm'},{'China':'flag flag-cn'},{'Colombia':'flag flag-co'},{'Costa Rica':'flag flag-cr'},{'Cuba':'flag flag-cu'},{'Cape Verde':'flag flag-cv'},{'Cura莽ao':'flag flag-cw'},{'Cyprus':'flag flag-cy'},{'Czech Republic':'flag flag-cz'},{'Germany':'flag flag-de'},{'Djibouti':'flag flag-dj'},{'Denmark':'flag flag-dk'},{'Dominica':'flag flag-dm'},{'Dominican Republic':'flag flag-do'},{'Algeria':'flag flag-dz'},{'Ecuador':'flag flag-ec'},{'Estonia':'flag flag-ee'},{'Egypt':'flag flag-eg'},{'Western Sahara':'flag flag-eh'},{'England':'flag flag-england'},{'Eritrea':'flag flag-er'},{'Spain':'flag flag-es'},{'Ethiopia':'flag flag-et'},{'European Union':'flag flag-eu'},{'Finland':'flag flag-fi'},{'Fiji':'flag flag-fj'},{'Falkland Islands (Malvinas)':'flag flag-fk'},{'Micronesia, Federated States Of':'flag flag-fm'},{'Faroe Islands':'flag flag-fo'},{'France':'flag flag-fr'},{'Gabon':'flag flag-ga'},{'United Kingdom':'flag flag-gb'},{'Grenada':'flag flag-gd'},{'Georgia':'flag flag-ge'},{'French Guiana':'flag flag-gf'},{'Guernsey':'flag flag-gg'},{'Ghana':'flag flag-gh'},{'Gibraltar':'flag flag-gi'},{'Greenland':'flag flag-gl'},{'Gambia':'flag flag-gm'},{'Guinea':'flag flag-gn'},{'Guadeloupe':'flag flag-gp'},{'Equatorial Guinea':'flag flag-gq'},{'Greece':'flag flag-gr'},{'South Georgia And South S.S.':'flag flag-gs'},{'Guatemala':'flag flag-gt'},{'Guam':'flag flag-gu'},{'Guinea-Bissau':'flag flag-gw'},{'Guyana':'flag flag-gy'},{'Hong Kong':'flag flag-hk'},{'Heard And Mc Donald Islands':'flag flag-hm'},{'Honduras':'flag flag-hn'},{'Croatia (Local Name: Hrvatska)':'flag flag-hr'},{'Haiti':'flag flag-ht'},{'Hungary':'flag flag-hu'},{'Iceland':'flag flag-ic'},{'Indonesia':'flag flag-id'},{'Ireland':'flag flag-ie'},{'Israel':'flag flag-il'},{'Isle Of Man':'flag flag-im'},{'India':'flag flag-in'},{'British Indian Ocean Territory':'flag flag-io'},{'Iraq':'flag flag-iq'},{'Iran (Islamic Republic Of)':'flag flag-ir'},{'Iceland':'flag flag-is'},{'Italy':'flag flag-it'},{'Jersey':'flag flag-je'},{'Jamaica':'flag flag-jm'},{'Jordan':'flag flag-jo'},{'Japan':'flag flag-jp'},{'Kenya':'flag flag-ke'},{'Kyrgyzstan':'flag flag-kg'},{'Cambodia':'flag flag-kh'},{'Kiribati':'flag flag-ki'},{'Comoros':'flag flag-km'},{'Saint Kitts And Nevis':'flag flag-kn'},{'Korea, D.P.R.O.':'flag flag-kp'},{'Korea, Republic Of':'flag flag-kr'},{'Kurdistan':'flag flag-kurdistan'},{'Kuwait':'flag flag-kw'},{'Cayman Islands':'flag flag-ky'},{'Kazakhstan':'flag flag-kz'},{'Laos':'flag flag-la'},{'Lebanon':'flag flag-lb'},{'Saint Lucia':'flag flag-lc'},{'Liechtenstein':'flag flag-li'},{'Sri Lanka':'flag flag-lk'},{'Liberia':'flag flag-lr'},{'Lesotho':'flag flag-ls'},{'Lithuania':'flag flag-lt'},{'Luxembourg':'flag flag-lu'},{'Latvia':'flag flag-lv'},{'Libyan Arab Jamahiriya':'flag flag-ly'},{'Morocco':'flag flag-ma'},{'Monaco':'flag flag-mc'},{'Moldova, Republic Of':'flag flag-md'},{'Montenegro':'flag flag-me'},{'Madagascar':'flag flag-mg'},{'Marshall Islands':'flag flag-mh'},{'Macedonia':'flag flag-mk'},{'Mali':'flag flag-ml'},{'Myanmar (Burma)':'flag flag-mm'},{'Mongolia':'flag flag-mn'},{'Macau':'flag flag-mo'},{'Northern Mariana Islands':'flag flag-mp'},{'Martinique':'flag flag-mq'},{'Mauritania':'flag flag-mr'},{'Montserrat':'flag flag-ms'},{'Malta':'flag flag-mt'},{'Mauritius':'flag flag-mu'},{'Maldives':'flag flag-mv'},{'Malawi':'flag flag-mw'},{'Mexico':'flag flag-mx'},{'Malaysia':'flag flag-my'},{'Mozambique':'flag flag-mz'},{'Namibia':'flag flag-na'},{'New Caledonia':'flag flag-nc'},{'Niger':'flag flag-ne'},{'Norfolk Island':'flag flag-nf'},{'Nigeria':'flag flag-ng'},{'Nicaragua':'flag flag-ni'},{'Netherlands':'flag flag-nl'},{'Norway':'flag flag-no'},{'Nepal':'flag flag-np'},{'Nauru':'flag flag-nr'},{'Niue':'flag flag-nu'},{'New Zealand':'flag flag-nz'},{'Oman':'flag flag-om'},{'Panama':'flag flag-pa'},{'Peru':'flag flag-pe'},{'French Polynesia':'flag flag-pf'},{'Papua New Guinea':'flag flag-pg'},{'Philippines':'flag flag-ph'},{'Pakistan':'flag flag-pk'},{'Poland':'flag flag-pl'},{'St. Pierre And Miquelon':'flag flag-pm'},{'Pitcairn':'flag flag-pn'},{'Puerto Rico':'flag flag-pr'},{'Palau':'flag flag-ps'},{'Portugal':'flag flag-pt'},{'Palau':'flag flag-pw'},{'Paraguay':'flag flag-py'},{'Qatar':'flag flag-qa'},{'Reunion':'flag flag-re'},{'Romania':'flag flag-ro'},{'Serbia':'flag flag-rs'},{'Russian Federation':'flag flag-ru'},{'Rwanda':'flag flag-rw'},{'Saudi Arabia':'flag flag-sa'},{'Solomon Islands':'flag flag-sb'},{'Seychelles':'flag flag-sc'},{'Scotland':'flag flag-scotland'},{'Sudan':'flag flag-sd'},{'Sweden':'flag flag-se'},{'Singapore':'flag flag-sg'},{'St. Helena':'flag flag-sh'},{'Slovenia':'flag flag-si'},{'Slovakia (Slovak Republic)':'flag flag-sk'},{'Sierra Leone':'flag flag-sl'},{'San Marino':'flag flag-sm'},{'Senegal':'flag flag-sn'},{'Somalia':'flag flag-so'},{'Somaliland':'flag flag-somaliland'},{'Suriname':'flag flag-sr'},{'South Sudan':'flag flag-ss'},{'Sao Tome And Principe':'flag flag-st'},{'El Salvador':'flag flag-sv'},{'South Georgia And The South Sandwich Island':'flag flag-sx'},{'Syrian Arab Republic':'flag flag-sy'},{'Swaziland':'flag flag-sz'},{'Turks And Caicos Islands':'flag flag-tc'},{'Chad':'flag flag-td'},{'French Southern Territories':'flag flag-tf'},{'Togo':'flag flag-tg'},{'Thailand':'flag flag-th'},{'Tajikistan':'flag flag-tj'},{'Tokelau':'flag flag-tk'},{'East Timor':'flag flag-tl'},{'Turkmenistan':'flag flag-tm'},{'Tunisia':'flag flag-tn'},{'Tonga':'flag flag-to'},{'Turkey':'flag flag-tr'},{'Trinidad And Tobago':'flag flag-tt'},{'Tuvalu':'flag flag-tv'},{'Taiwan, Province Of China':'flag flag-tw'},{'Tanzania, United Republic Of':'flag flag-tz'},{'Ukraine':'flag flag-ua'},{'Uganda':'flag flag-ug'},{'U.S. Minor Islands':'flag flag-um'},{'United States':'flag flag-us'},{'Uruguay':'flag flag-uy'},{'Uzbekistan':'flag flag-uz'},{'Holy See (Vatican City State)':'flag flag-va'},{'Saint Vincent And The Grenadines':'flag flag-vc'},{'Venezuela':'flag flag-ve'},{'Virgin Islands (British)':'flag flag-vg'},{'Virgin Islands (U.S.)':'flag flag-vi'},{'Vietnam':'flag flag-vn'},{'Vanuatu':'flag flag-vu'},{'Wales':'flag flag-wales'},{'Wallis And Futuna Islands':'flag flag-wf'},{'Samoa':'flag flag-ws'},{'Yemen':'flag flag-ye'},{'Mayotte':'flag flag-yt'},{'South Africa':'flag flag-za'},{'Zanzibar':'flag flag-zanzibar'},{'Zambia':'flag flag-zm'},{'Zimbabwe':'flag flag-zw'}];
            mapping.forEach(function(country) {
                if(country.hasOwnProperty(banner)){
                    elem.attr('class', country[banner]);
                }
            });
        });
    };
});



