package reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import writable.DayStatsWritable;


public class HiLowReducer extends Reducer<Text,DayStatsWritable,Text,Text>{
	
	private String date;
	private String ticker; 
	private Text newKey = new Text(); 
	private Text valout = new Text(); 
	
	private final double startCapitalDefault = 5000.00; 
	
	private final static int pricesSize = 4;
	private final static int open = 0; 
	private final static int high = 1; 
	private final static int low = 2; 
	private final static int close = 3;
	private double[] prices = new double[pricesSize]; 
	
	private final static int numHiLows = 7; 
	private final static int twentyHigh = 0; 
	private final static int thirtyHigh = 1; 
	private final static int fittyFiveHigh = 2; 
	private final static int twoFittyHigh = 3; 
	private final static int tenLow = 4; 
	private final static int twentyLow = 5; 
	private final static int thirtyLow = 6; 
	private double[] daysHiLows = new double[numHiLows]; 
	
	
	private final int nSize = 3;
	private final int halfN = 0; 
	private final int oneN = 1; 
	private final int twoN = 2; 
	private double[] nVals = new double[nSize];
	
	
	private final int posSize = 9; 
	private final int HiLow_20_10 = 0;
	private final int HiLow_30_10 = 1;
	private final int HiLow_55_10 = 2;
	private final int HiLow_250_10 = 3; 
	private final int HiLow_30_20 = 4; 
	private final int HiLow_55_20 = 5; 
	private final int HiLow_250_20 = 6; 
	private final int HiLow_55_30 = 7;
	private final int HiLow_250_30 = 8; 

	private boolean[] posEntered = new boolean[posSize*3];
//	private boolean[] entrySignal = new boolean[posSize*3]; 
//	private boolean[] exitSignal = new boolean[posSize*3]; 
	private double[] entryPrice = new double[posSize*3]; 
	private double[] exitPrice = new double[posSize*3]; 
	private int[] shares = new int[posSize*3];
	private double[] stopPrice = new double[posSize*3]; 
	private double[] startCapital = new double[posSize*3]; 
	private double[] entryCapital = new double[posSize*3]; 
	private double[] exitCapital = new double[posSize*3];
	private double[] realizedGains = new double[posSize*3]; 
	private String[] lineBuilder = new String[posSize*3]; 
	
	HashMap<String, ArrayList<String>> results = new HashMap<String, ArrayList<String>>();
	
	public void reduce(Text key, Iterable<DayStatsWritable> values, Context context) throws IOException, InterruptedException{
		
		ticker = key.toString().trim(); 
		
		results.clear();
		
		for(int i = 0; i < posSize * 3; i++){
			posEntered[i] = false; 
//			entrySignal[i] = false; 
//			exitSignal[i] = false; 
			entryPrice[i] = 0.0;
			exitPrice[i] = 0.0; 
			shares[i] = 0; 
			stopPrice[i] = 0.0; 
			startCapital[i] = startCapitalDefault;
			realizedGains[i] = 0.0; 
			entryCapital[i] = 0.0;
			exitCapital[i] = 0.0; 
		}
		for(int i = 0; i < numHiLows; i++){
			daysHiLows[i] = 0.0; 
		}
		for(int i = 0; i < nSize; i++) nVals[i] = 0.0; 
		for(int i = 0; i < pricesSize; i++) prices[i] = 0.0; 
		
		for(DayStatsWritable val: values){
			
			date = val.getDate().toString().trim(); 
			
			prices[open] = val.getOpen().get(); 
			prices[high] = val.getHigh().get(); 
			prices[low] = val.getLow().get(); 
			prices[close] = val.getClose().get();
			
			nVals[halfN] = (val.getnValue().get() / 2); 
			nVals[oneN] = (val.getnValue().get()); 
			nVals[twoN] = (val.getnValue().get() * 2); 
			
			daysHiLows[twentyHigh] = val.getTwentyHigh().get(); 
			daysHiLows[thirtyHigh] = val.getThirtyHigh().get(); 
			daysHiLows[fittyFiveHigh] = val.getFiftyFiveHigh().get();
			daysHiLows[twoFittyHigh] = val.getTwoFiftyHigh().get(); 
			daysHiLows[tenLow] = val.getTenLow().get(); 
			daysHiLows[twentyLow] = val.getTwentyLow().get();
			daysHiLows[thirtyLow] = val.getThirtyLow().get();
			
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(twentyHigh, tenLow, i, HiLow_20_10);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(thirtyHigh, tenLow, i, HiLow_30_10);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(fittyFiveHigh, tenLow, i, HiLow_55_10);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(twoFittyHigh, tenLow, i, HiLow_250_10);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(thirtyHigh, twentyLow, i, HiLow_30_20);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(fittyFiveHigh, twentyLow, i, HiLow_55_20);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(twoFittyHigh, twentyLow, i, HiLow_250_20);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(fittyFiveHigh, thirtyLow, i, HiLow_55_20);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(twoFittyHigh, thirtyLow, i, HiLow_250_30);
			}

			
		}
		
		for(String r: results.keySet()){
			
			int posIndex = Integer.parseInt(r.substring(5, r.indexOf(":")).trim());
			int exitLow = Integer.parseInt((r.charAt(3)+"").trim());
			if(!lineBuilder[posIndex].equals("")){
				exitPosition(exitLow,posIndex); 
			}
			for(String l: results.get(r)){
				context.write(new Text(r), new Text(l));
			}
		}
	}
	
	public void analyzeTradeDate(int entryHi, int exitLow, int nVal, int HiLowIndex){
		
		int trueIndex = (HiLowIndex * 3) + nVal;  
		
		double thisHi = daysHiLows[entryHi];
		double thisLow = daysHiLows[exitLow];

		double thisN = nVals[nVal]; 
		
		// IF ANY OF THESE CONDITIONS EXIST
		// 		Not enough data to run yet so just return 
		if(Math.abs(thisN) == 99999.99) return;
		if(Math.abs(thisHi) == 99999.99) return;
		if(Math.abs(thisLow) == 99999.99) return; 
		
		newKey.set("HL"+entryHi+""+exitLow+""+nVal+""+HiLowIndex+":"+ticker);
		
		if(!results.containsKey(newKey.toString())){
			ArrayList<String> a = new ArrayList<String>(); 
			results.put(newKey.toString(), a); 
		}
		
		// If Position entered check for stop conditions
		// If stop condtion not met check for exit condition
		
		if(posEntered[trueIndex]){
			
			/************************************************
			 *  Three possibilities on exit: 
			 *  	1: price opens below stop indicating stop should be taken on open
			 *  	2: price dips below target exit price
			 *  	3: price goes below stop price mid day and stop must be taken. 
			 ************************************************/
			if(prices[low] <= daysHiLows[exitLow]){
				exitPosition(exitLow, trueIndex); 
				return;
			}
		}else if(!posEntered[trueIndex]){
			
			/***************************************************
			 *  If position not entered:
			 *  	1: If high is greater than entry price enter a position
			 ***************************************************/
			if(prices[high] >= daysHiLows[entryHi])
				enterPosition(entryHi, trueIndex, nVal);
			
		}
		
	}
	
	private void enterPosition(int entryHi, int trueIndex, int nVal) {
		
		posEntered[trueIndex] = true; 
		
		if(prices[open] >= daysHiLows[entryHi]){
			// entry price = open price
			entryPrice[trueIndex] = prices[open]; 
		}else{
			//entry price = new high
			entryPrice[trueIndex] = daysHiLows[entryHi]; 
		}
		shares[trueIndex] = (int)(startCapital[trueIndex] / entryPrice[trueIndex]); 
		stopPrice[trueIndex] = entryPrice[trueIndex] - nVals[nVal];
		entryCapital[trueIndex] = entryPrice[trueIndex] * shares[trueIndex]; 
		
		lineBuilder[trueIndex] = date+"\t"+startCapital[trueIndex]+"\t"+entryPrice[trueIndex]
				+"\t"+shares[trueIndex]+"\t"+nVals[nVal]+"\t"+stopPrice[trueIndex]+"\t";
	}

	private void exitPosition(int exitLow, int trueIndex){
		
		posEntered[trueIndex] = false; 
		
		String action = "";
		
		if(prices[open] <= daysHiLows[exitLow]){
			exitPrice[trueIndex] = prices[open]; 
			action = "SELL"; 
		}else if(prices[open] <= stopPrice[trueIndex]){
			exitPrice[trueIndex] = prices[open]; 
			action = "STOP";
		}else if(prices[low] <= daysHiLows[exitLow]){
			exitPrice[trueIndex] = daysHiLows[exitLow]; 
			action = "SELL";
		}else if(prices[low] <= stopPrice[trueIndex]){
			exitPrice[trueIndex] = stopPrice[trueIndex]; 
			action = "STOP"; 
		}else{
			exitPrice[trueIndex] = prices[close];
			action = "EOF_SELL";
		}
		
		exitCapital[trueIndex] = exitPrice[trueIndex] * (double)shares[trueIndex];
		
		realizedGains[trueIndex] = exitCapital[trueIndex] - entryCapital[trueIndex]; 
		startCapital[trueIndex] = startCapital[trueIndex] + realizedGains[trueIndex]; 
		
		double percentGain = (exitCapital[trueIndex] / entryCapital[trueIndex] - 1.0) * 100.00; 
		
		lineBuilder[trueIndex] += (action+"\t"+date+"\t"+exitPrice[trueIndex]+"\t"+
				realizedGains[trueIndex]+"\t"+percentGain+"\t"+startCapital[trueIndex]); 
		
		ArrayList<String> a = results.get(newKey.toString());
		a.add(lineBuilder[trueIndex]);
		results.put(newKey.toString(), a);
		entryPrice[trueIndex] = 0.0; 
		exitPrice[trueIndex] = 0.0; 
		shares[trueIndex] = 0; 
		stopPrice[trueIndex] = 0.0;
		entryCapital[trueIndex] = 0.0; 
		exitCapital[trueIndex] = 0.0; 
		realizedGains[trueIndex] = 0.0; 
		lineBuilder[trueIndex] = ""; 
	}

}
