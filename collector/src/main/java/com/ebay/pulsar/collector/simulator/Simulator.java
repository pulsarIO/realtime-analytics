/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.simulator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event/Processor", description = "Event Simulator")
public class Simulator extends AbstractEventProcessor implements
		InitializingBean {

	private Timer timer = new Timer("Timer");
	private TimerTask timerTask;

	private String simulatorFilePath;
	private String ipFilePath;
	private String uaFilePath;
	private String itmFilePath;
	private int siCount = 1000;
	private int minVolume = 10;
	private double peakTimes = 100;
	private boolean batchMode = true;
	
	private HttpClient m_client;
	private PostMethod m_method;
	private long m_batchSize;
	private String m_payload = "";
	private boolean m_runFlag = true;
	private List m_guidList = new ArrayList<RawSource>();

	static String EVENTTYPE = "PulsarRawEvent";
	static String URL = "/pulsar/ingest/";
	static String BATCH_URL = "/pulsar/batchingest/";
	static String NODE = "http://localhost:8080";

	static List<String> m_ipList = new ArrayList<String>();
	static List<String> m_uaList = new ArrayList<String>();
	static List<String> m_itemList = new ArrayList<String>();

	private class SimulatorTimingTask extends TimerTask {
		public void run() {
			generate();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);
		timerTask = new SimulatorTimingTask();
		timer.schedule(timerTask, 30 * 1000);
		init();
	}

	private void init() {
		initList(m_ipList, ipFilePath);
		initList(m_uaList, uaFilePath);
		initList(m_itemList, itmFilePath);
		initGUIDList();

		String finalURL = "";
		if (batchMode) {
			finalURL = BATCH_URL;
		} else {
			finalURL = URL;
		}
		m_payload = readFromResource();
		HttpClientParams clientParams = new HttpClientParams();
		clientParams.setSoTimeout(60000);
		m_client = new HttpClient(clientParams);
		m_method = new PostMethod(NODE + finalURL + EVENTTYPE);
		m_method.setRequestHeader("Connection", "Keep-Alive");
		m_method.setRequestHeader("Accept-Charset", "UTF-8");
	}

	public void setSimulatorFilePath(String filename) {
		simulatorFilePath = filename;
	}

	public String getSimulatorFilePath() {
		return simulatorFilePath;
	}

	public void setIpFilePath(String filename) {
		ipFilePath = filename;
	}

	public String getIpFilePath() {
		return ipFilePath;
	}

	public void setUaFilePath(String filename) {
		uaFilePath = filename;
	}

	public String getUaFilePath() {
		return uaFilePath;
	}
	
	public void setItmFilePath(String filename) {
		itmFilePath = filename;
	}

	public String getItmFilePath() {
		return itmFilePath;
	}

	public void setSiCount(int count) {
		siCount = count;
	}

	public int getSiCount() {
		return siCount;
	}

	public void setMinVolume(int rate) {
		minVolume = rate;
	}

	public int getMinVolume() {
		return minVolume;
	}

	public void setPeakTimes(double rate) {
		peakTimes = rate;
	}

	public double getPeakTimes() {
		return peakTimes;
	}

	public void setBatchMode(boolean flag) {
		batchMode = flag;
	}

	public boolean getBatchMode() {
		return batchMode;
	}

	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getPendingEvents() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutDown() {
		if (m_method != null)
			m_method.releaseConnection();
	}

	private long trafficVolume(int baseVolume, double peakTimes, int sec){
        if(baseVolume<0)
            throw new IllegalArgumentException("baseVolume should be >= 0!");
        if(peakTimes <= 0)
            throw new IllegalArgumentException("peakTimes should be > 0!");
        return (long)(baseVolume + (baseVolume * (peakTimes - 1)) * Math.sin(sec*Math.PI/60));
    }

	private void adjustBatchSize() {
		Date date = new Date();
		int secondValue = date.getSeconds();
		m_batchSize = trafficVolume(minVolume, peakTimes, secondValue);
	}
	
	@ManagedOperation
	public void generate() {
		while (true) {
			adjustBatchSize();
			try {
				if (m_runFlag) {
					if (batchMode) {
						m_method.setRequestBody(buildBatchPayload(m_payload));
					} else {
						m_method.setRequestBody(m_payload);
					}
					m_client.executeMethod(m_method);
				}
			} catch (HttpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				m_method.releaseConnection();
				m_method.removeRequestHeader("Content-Length");
			}
			Random random = new Random();
			long sleep = random.nextInt(10000);
			System.out.println("sleep = "+ sleep);
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
	}

	@ManagedOperation
	public void start() {
		m_runFlag = true;
	}

	@ManagedOperation
	@Override
	public void pause() {
		m_runFlag = false;
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub

	}

	private String readFromResource() {
		FileReader in;
		String payload = "";
		try {
			File file = new File(simulatorFilePath);
			in = new FileReader(file);
			BufferedReader r = new BufferedReader(in);
			String line;
			try {
				while ((line = r.readLine()) != null) {
					payload += line;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					r.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return payload;
	}

	private String buildBatchPayload(String payload) {
		Random random = new Random();
		StringBuffer strBuffer = new StringBuffer("[");
		for (int i = 0; i < m_batchSize; i++) {
			int randomInt = random.nextInt(siCount);
			RawSource source = (RawSource) m_guidList.get(randomInt);
			strBuffer.append(replaceSource(payload, source));
			if (i < m_batchSize - 1)
				strBuffer.append(",");
		}
		strBuffer.append("]");
		return strBuffer.toString();
	}

	private String replaceSource(String payload, RawSource source) {
		Random random = new Random();
		String payload1 = payload.replace("${siValue}",
				source.getIndicatorString());
		String payload2 = payload1.replace("${ipValue}", source.getGeoString());
		String payload3 = payload2.replace("${uaValue}",
				source.getDeviceString());
		String payload4 = payload3.replace("${ctValue}",
				String.valueOf(System.currentTimeMillis()));
		String[] items = m_itemList.get(random.nextInt(m_itemList.size() - 1)).split(":");
		String payload5 = payload4.replace("${itmTitle}", items[0]);
		String payload6 = payload5.replace("${itmPrice}", String.valueOf(((double)random.nextInt(10000)*36)/100.00));
		String payload7 = payload6.replace("${campaignName}", "Campaign - " + String.valueOf(random.nextInt(20)));
		String payload8 = payload7.replace("${cmapaignGMV}", String.valueOf(((double)random.nextInt(100000)*7)/100.00));
		String payload9 = payload8.replace("${cmapaignQuantity}", String.valueOf(random.nextInt(100)));
		return payload9;
	}

	private void initGUIDList() {
		Random random = new Random();
		for (int i = 0; i < siCount; i++) {
			String key = java.util.UUID.randomUUID().toString();
			m_guidList.add(new RawSource(key, m_ipList.get(random
					.nextInt(m_ipList.size() - 1)), m_uaList.get(random
					.nextInt(m_uaList.size() - 1))));
		}
	}

	private void initList(List<String> list, String filename) {
		FileReader in;
		try {
			File file = new File(filename);
			in = new FileReader(file);
			BufferedReader r = new BufferedReader(in);
			String line;
			try {
				while ((line = r.readLine()) != null) {
					list.add(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					r.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private class RawSource {

		String geoString;
		String deviceString;
		String indicatorString;

		public RawSource(String indicator, String geo, String device) {
			indicatorString = indicator;
			geoString = geo;
			deviceString = device;
		}

		public String getGeoString() {
			return geoString;
		}

		public void setGeoString(String geo) {
			geoString = geo;
		}

		public String getDeviceString() {
			return deviceString;
		}

		public void setDeviceString(String device) {
			deviceString = device;
		}

		public String getIndicatorString() {
			return indicatorString;
		}

		public void setIndicatorString(String indicator) {
			indicatorString = indicator;
		}

	}
}
