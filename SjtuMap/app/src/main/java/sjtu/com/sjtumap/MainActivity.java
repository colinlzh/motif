package sjtu.com.sjtumap;
import android.graphics.Color;
import android.os.Environment;
import android.os.Handler;
import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.NumberPicker;
import android.widget.Toast;

import com.baidu.mapapi.SDKInitializer;
import com.baidu.mapapi.map.Gradient;
import com.baidu.mapapi.map.HeatMap;
import com.baidu.mapapi.map.InfoWindow;
import com.baidu.mapapi.map.MapStatusUpdate;
import com.baidu.mapapi.map.MapStatusUpdateFactory;
import com.baidu.mapapi.map.MapView;
import com.baidu.mapapi.map.WeightedLatLng;
import com.baidu.mapapi.model.LatLng;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MainActivity extends ActionBarActivity {
    final static String TAG = "LOGCAT";
    private MapView mMapView = null;
    private InfoWindow mInfoWindow;
    private com.baidu.mapapi.map.BaiduMap mBaiduMap = null;
    private HashMap<String,  HashMap<Integer, Integer>> map = null;
    private HashMap<String,double[]> map2=null;
    private Button b = null;
    private Button c = null;
    private NumberPicker n=null;
    HeatMap heatmap=null;
    int i=0;
    int j=0;
    String s="";
    String[] t=null;
    int nodeIndex = -1;//节点索引,供浏览节点时使用
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        SDKInitializer.initialize(getApplicationContext());
        setContentView(R.layout.activity_main);
        b = (Button) findViewById(R.id.shit);
        c = (Button) findViewById(R.id.shit2);
        n = (NumberPicker) findViewById(R.id.num);
        n.setMaxValue(5);
        n.setMinValue(0);

////        b.setVisibility(View.INVISIBLE);
        map = file();
        map2=file2();
        mMapView = (MapView) findViewById(R.id.bmapView);
        mBaiduMap = mMapView.getMap();
        MapStatusUpdate u = MapStatusUpdateFactory.zoomTo(10.0f);
        mBaiduMap.setMapStatus(MapStatusUpdateFactory.newLatLng(new LatLng(31.083236,121.394725)));
        mBaiduMap.animateMapStatus(u);
        b.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                heatmap.removeHeatMap();
//                for(String name:map2.keySet()){
//                    LatLng ptCenter=new LatLng(map2.get(name)[0],map2.get(name)[1]);
//                    mBaiduMap.addOverlay(new MarkerOptions().position(ptCenter)
//                            .icon(BitmapDescriptorFactory
//                                    .fromResource(R.drawable.shit)).perspective(true));
//                }
            }
        });
        c.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if(heatmap!=null) heatmap.removeHeatMap();
                int time=0;
                switch (n.getValue()){
                    case 0:
                        time=4;
                        break;
                    case 1:
                        time=8;
                        break;
                    case 2:
                        time=10;
                        break;
                    case 3:
                        time=16;
                        break;
                    case 4:
                        time=18;
                        break;
                    case 5:
                        time=22;
                        break;
                }
//                int[] DEFAULT_GRADIENT_COLORS = {Color.rgb(255, 255, 0),Color.rgb(255, 200, 0),Color.rgb(255, 155, 0) ,Color.rgb(255, 100, 0),Color.rgb(255, 55, 0),Color.rgb(255, 0, 0) };
                int[] DEFAULT_GRADIENT_COLORS = {Color.rgb(255, 255, 0),Color.rgb(255, 0, 0) };
//设置渐变颜色起始值
                float[] DEFAULT_GRADIENT_START_POINTS = { 0.2f, 1f };
//                float[] DEFAULT_GRADIENT_START_POINTS = { 0f,0.2f, 0.4f,0.6f,0.8f,1f };
//构造颜色渐变对象1
                List<WeightedLatLng> l = new ArrayList<WeightedLatLng>();
                for (String n:map.keySet()) {
                    if(!map2.containsKey(n)|!map.get(n).containsKey(time)) {
                        Log.e(TAG,n);
                        continue;
                    }
                    WeightedLatLng ll = new WeightedLatLng(new LatLng(map2.get(n)[0], map2.get(n)[1]),map.get(n).get(time));
                    l.add(ll);
                }
                Gradient gradient = new Gradient(DEFAULT_GRADIENT_COLORS, DEFAULT_GRADIENT_START_POINTS);


                heatmap = new HeatMap.Builder()
                        .weightedData(l)
                        .gradient(gradient)
                        .radius(20)
                        .build();
//在地图上添加热力图
                mBaiduMap.addHeatMap(heatmap);
            }
        });
        //设置渐变颜色值



    }
    @Override
    protected void onDestroy() {
        super.onDestroy();
        //在activity执行onDestroy时执行mMapView.onDestroy()，实现地图生命周期管理
        mMapView.onDestroy();
    }
    @Override
    protected void onResume() {
        super.onResume();
        //在activity执行onResume时执行mMapView. onResume ()，实现地图生命周期管理
        mMapView.onResume();
    }

    @Override
    protected void onPause() {
        super.onPause();
        //在activity执行onPause时执行mMapView. onPause ()，实现地图生命周期管理
        mMapView.onPause();
    }

    public HashMap<String, HashMap<Integer, Integer>> file() {
        HashMap<String, HashMap<Integer, Integer>> m = new HashMap<String, HashMap<Integer, Integer>>();
        String fileName = "heat.csv"; //文件名字
        try {
            InputStream in = getResources().getAssets().open(fileName);
            Reader reader = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(reader);
            String line = "";
            while ((line = br.readLine()) != null) {
                if(line.contains("null")) continue;
                String[] t=line.split(",");
                if(m.containsKey(t[0])){
                    HashMap<Integer, Integer> a=m.get(t[0]);
                    a.put(Integer.valueOf(t[1]),Integer.valueOf(t[2]));
                    m.put(t[0],a);
                }else{
                    HashMap<Integer, Integer> a = new HashMap<>();
                    a.put(Integer.valueOf(t[1]),Integer.valueOf(t[2]));
                    m.put(t[0], a);
                }

            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return m;
    }
    public HashMap<String, double[]> file2() {
        HashMap<String, double[]> m = new HashMap<String, double[]>();
        String fileName = "MapCount.csv"; //文件名字
        String[] res = new String[2];
        try {
            //得到资源中的asset数据流
            InputStream in = getResources().getAssets().open(fileName);
            Reader reader = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(reader);
            String line = "";
            while ((line = br.readLine()) != null) {
                if(!line.contains(".")) continue;
                double[] a = {Double.valueOf(line.split(",")[5]),Double.valueOf(line.split(",")[4])};
                m.put(line.split(",")[1], a);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return m;
    }
    //读文件

    private void write(String content,Boolean t) {
        if(!Environment.getExternalStorageState().equals(Environment.MEDIA_MOUNTED)){
            Toast.makeText(getApplicationContext(), "读取失败，SD存储卡不存在！", Toast.LENGTH_LONG).show();
            return;
        }
        //初始化File
        String path=Environment.getExternalStorageDirectory().toString()
                +File.separator
                +"shit.txt";
        try{
            FileOutputStream fout = new FileOutputStream(path,t);
            byte [] bytes = content.getBytes();
            fout.write(bytes);
            fout.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }


}
