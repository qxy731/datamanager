package com.myboot.dataprocess.process.ignite;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import com.myboot.dataprocess.common.MyConstants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyIgniteRepository {
	
	
	private static Ignite ignite;
	private static IgniteConfiguration igniteCfg = getIgniteConfiguration();
	
	
	/**
	 * 根据文件路径和文件编号解析文件
	 * 
	 * @param filePath
	 * @param fileNo
	 */
	static AtomicLong sequence = new AtomicLong(0);
	static {
		new Thread(()->{
			while(true) {
				try {
					Thread.sleep(10_000);
					log.info("已流式导入： " + sequence);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
	
	private static Ignite getIgnite() {
		if(ignite == null) {
			TcpDiscoverySpi spi = new TcpDiscoverySpi();
		    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
		    ipFinder.setAddresses(Arrays.asList(MyConstants.IGNITE_SERVER_IP));
		    //TcpDiscoveryVmIpFinder ipFinder1 = new TcpDiscoveryMulticastIpFinder().setAddresses(Collections.singleton(MyConstants.IGNITE_SERVER_IP));
		    spi.setIpFinder(ipFinder);
		    igniteCfg.setDiscoverySpi(spi);
		    Ignition.setClientMode(true);
		    ignite = Ignition.start(igniteCfg); 
		}
	    return ignite;
	}
	
	private static IgniteConfiguration getIgniteConfiguration() {
	    IgniteConfiguration igniteCfg = new IgniteConfiguration();
	    igniteCfg.setClientMode(true);
	    /*String logPath = "src/main/resources/log4j2.xml";
	    igniteCfg.setGridLogger(new Log4J2Logger(logPath));*/
	    return igniteCfg;
	}
	
	public static IgniteCache<String, Object> getIgniteCache() {
	    // 5 GB maximum size
		ignite = getIgnite();
		//igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(MyConstants.IGNITE_CACHE_SIZE_DEFAULT);
	    CacheConfiguration<String, Object> cacheCfg = new CacheConfiguration<>(MyConstants.IGNITE_CACHE_KEY_DEFAULT);
	    cacheCfg.setCopyOnRead(false);
	    //cacheCfg.setCacheMode(cacheMode)
	    IgniteCache<String, Object> cache = ignite.getOrCreateCache(cacheCfg);
	    return cache;
	}
	
	public static void save2Ignite(List<ImmutablePair<String, Object>> dataList) throws Exception{
		ignite = getIgnite();
		//分布式id生成器
        //IgniteAtomicSequence sequence = ignite.atomicSequence(MyConstants.IGNITE_ATOMIC_SEQUENCE, 0, true);
	 	try (IgniteDataStreamer<String, VersionCache> streamer = ignite.dataStreamer(MyConstants.IGNITE_CACHE_KEY_DEFAULT)) {
	    	streamer.allowOverwrite(true);
	    	dataList.stream().forEach(immutablePairObj -> {
	    		VersionCache vc = (VersionCache)immutablePairObj.getValue();
	    		String key = immutablePairObj.getKey();
	    		streamer.addData(key, vc);
	    		sequence.incrementAndGet();
	    	});
	        //将流里面的剩余数据压进ignite
	        streamer.flush();
	 	}
	}
	
}