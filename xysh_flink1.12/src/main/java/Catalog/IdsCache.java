//package Catalog;
//
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.google.common.cache.CacheBuilder;
//import com.google.common.cache.CacheLoader;
//import com.google.common.cache.LoadingCache;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//
//public class IdsCache {
//
//    private LoadingCache<String, Map<String,String>> caches = CacheBuilder.newBuilder()
//                                                                          // 1分钟后刷新
//                                                                          .refreshAfterWrite(1, TimeUnit.MINUTES)
//                                                                          .build(new CacheLoader<String, Map<String,String>>() {
////                                                                              @Override
////                                                                              public Map<String, String> load(String s) throws Exception {
////                                                                                  return null;
////                                                                              }
//                                                                              @Override
//                                                                              public Map<String,String> load(String key) throws Exception {
//                                                                                  final String url = "http://域名/api/v1/logs?zone=cnaz1";
//                                                                                  JSONObject jsonObject = JSONObject.parseObject(GetHttpData.getData(url));
//                                                                                  String list  = jsonObject.get("ids").toString();
//                                                                                  List<String> list1=  JSONObject.parseArray(list).toJavaList(String.class);
//                                                                                  HashMap<String, String> map = new HashMap<>();
//                                                                                  for (String s : list1) {
//                                                                                      map.put(s,s);
//                                                                                  }
//                                                                                  return map;
//                                                                              }
//                                                                          });
//    public LoadingCache<String, Map<String,String>> getCache(){
//        return this.caches;
//    }
//    public static void main(String[] args) {
//        LoadingCache<String, Map<String, String>> caches = new IdsCache().getCache();
//        try {
//            while (true)
//            {
//                Map<String,String> map2 = caches.get("ids");
//                System.out.println(map2.size());
//                Thread.sleep(2000);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//}
