package reduce; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import writable.CompositeKey;
import writable.DayStatsWritable;

public class EMAReducer extends Reducer<CompositeKey,DayStatsWritable,Text,Text> { 

	private String date;
	private String ticker; 
	private Text newKey = new Text(); 
	private Text valout = new Text(); 

	private String prevTicker = ""; 

	private final double startCapitalDefault = 5000.00; 

	private final int pricesSize = 4;
	private final int open = 0; 
	private final int high = 1; 
	private final int low = 2; 
	private final int close = 3;
	private double[] prices = new double[pricesSize]; 


	private final int emaSize = 6; 
	private final int ema12 = 0; 
	private final int ema26 = 1;
	private final int ema50 = 2; 
	private final int ema75 = 3;
	private final int ema150 = 4; 
	private final int ema200 = 5; 
	private double[] thisEMA = new double[emaSize]; 
	private double[] prevEMA = new double[emaSize];

	private final int nSize = 3;
	private final int halfN = 0; 
	private final int oneN = 1; 
	private final int twoN = 2; 
	private double[] nVals = new double[nSize];

	private final int posSize = 10; 
	private final int pos12_26 = 0;
	private final int pos12_50 = 1;
	private final int pos12_75 = 2;
	private final int pos26_50 = 3; 
	private final int pos26_75 = 4; 
	private final int pos26_150 = 5; 
	private final int pos50_150 = 6; 
	private final int pos50_200 = 7;
	private final int pos75_150 = 8; 
	private final int pos75_200 = 9; 
	private boolean[] posEntered = new boolean[posSize*3];
	private boolean[] entrySignal = new boolean[posSize*3]; 
	private boolean[] exitSignal = new boolean[posSize*3]; 
	private double[] entryPrice = new double[posSize*3]; 
	private double[] exitPrice = new double[posSize*3]; 
	private int[] shares = new int[posSize*3];
	private double[] stopPrice = new double[posSize*3]; 
	private double[] startCapital = new double[posSize*3]; 
	private double[] entryCapital = new double[posSize*3]; 
	private double[] exitCapital = new double[posSize*3];
	private double[] realizedGains = new double[posSize*3]; 
	private String[] lineBuilder = new String[posSize*3]; 

	private HashMap<String, ArrayList<String>> results = new HashMap<String, ArrayList<String>>(); 
	//	private ArrayList<String> resultsOrder = new ArrayList<String>(); 

	public void reduce(CompositeKey key, Iterable<DayStatsWritable> values, Context context) throws IOException, InterruptedException{

		//		// Testing for proper sorting...
		//		for(DayStatsWritable val: values){
		//			context.write(key.getTicker(), new Text(val.toString()));
		//		}

		ticker = key.getTicker().toString().trim(); 


		if(!key.getTicker().toString().trim().equals(prevTicker)){


			for(String r: results.keySet()){
				int posIndex = -1;
				try{
					posIndex = Integer.parseInt(r.substring(5, r.indexOf(":")).trim());
					if(!lineBuilder[posIndex].equals("")){
						exitPosition(0,0,0,posIndex,context); 
					}
					for(String l: results.get(r)){
						context.write(new Text(r), new Text(l));
					}
				} catch(Exception e) {
					System.err.println("\n\n**********************************************");
					System.err.println("Error parsing stock data");
					System.err.println("Line position length: " + lineBuilder.length);
					System.err.println("Enter position index: " + posIndex);
					e.printStackTrace();
					System.err.println("**********************************************\n\n");
					// System.exit(0);
				}

			}


			results.clear();

			for(int i = 0; i < posSize * 3; i++){
				posEntered[i] = false; 
				entrySignal[i] = false; 
				exitSignal[i] = false; 
				entryPrice[i] = 0.0;
				exitPrice[i] = 0.0; 
				shares[i] = 0; 
				stopPrice[i] = 0.0; 
				startCapital[i] = startCapitalDefault;
				realizedGains[i] = 0.0; 
				entryCapital[i] = 0.0;
				exitCapital[i] = 0.0; 
			}
			for(int i = 0; i < emaSize; i++){
				thisEMA[i] = 0.0;
				prevEMA[i] = 0.0; 
			}
			for(int i = 0; i < nSize; i++) nVals[i] = 0.0; 
			for(int i = 0; i < pricesSize; i++) prices[i] = 0.0; 

		}


		for(DayStatsWritable val: values){

			date = val.getDate().toString().trim(); 

			prices[open] = val.getOpen().get(); 
			prices[high] = val.getHigh().get(); 
			prices[low] = val.getLow().get(); 
			prices[close] = val.getClose().get();

			nVals[halfN] = (val.getnValue().get() / 2); 
			nVals[oneN] = (val.getnValue().get()); 
			nVals[twoN] = (val.getnValue().get() * 2); 

			thisEMA[ema12] = val.getEMA12().get(); 
			thisEMA[ema26] = val.getEMA26().get(); 
			thisEMA[ema50] = val.getEMA50().get();
			thisEMA[ema75] = val.getEMA75().get(); 
			thisEMA[ema150] = val.getEMA150().get(); 
			thisEMA[ema200] = val.getEMA200().get();

			// Calls to process data

			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema26, i, pos12_26, context);
			}

			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema50, i, pos12_50, context);
			}

			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, i, pos12_75, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, i, pos26_50, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema75, i, pos26_75, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema150, i, pos26_150, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, i, pos50_150, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema200, i, pos50_200, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema75, ema150, i, pos75_150, context);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema75, ema200, i, pos75_200, context);
			}

			prevEMA[ema12] = thisEMA[ema12]; 
			prevEMA[ema26] = thisEMA[ema26]; 
			prevEMA[ema50] = thisEMA[ema50];
			prevEMA[ema75] = thisEMA[ema75]; 
			prevEMA[ema150] = thisEMA[ema150]; 
			prevEMA[ema200] = thisEMA[ema200];
		}
		
		prevTicker = key.getTicker().toString().trim(); 

	}

	public void analyzeTradeDate(int shortEMA, int longEMA, int nVal, int posIndex, Context context){

		int trueIndex = (posIndex * 3) + nVal;  

		double thisShort = thisEMA[shortEMA];
		double prevShort = prevEMA[shortEMA];
		double thisLong = thisEMA[longEMA];
		double prevLong = prevEMA[longEMA];
		double thisN = nVals[nVal]; 

		double thisSpread = thisShort - thisLong;
		double prevSpread = prevShort - prevLong;

		// IF ANY OF THESE CONDITIONS EXIST
		// 		Not enough data to run yet so just return 
		if(Math.abs(thisN) == 99999.99) return;
		if(Math.abs(thisLong) == 99999.99) return;
		if(Math.abs(prevLong) == 99999.99) return; 

		newKey.set("EM"+shortEMA+""+longEMA+""+nVal+""+trueIndex+":"+ticker);
		//		String hashKey = shortEMA+""+longEMA+""+nVal+""+posIndex+":"+ticker;

		if(!results.containsKey(newKey.toString())){
			ArrayList<String> a = new ArrayList<String>(); 
			results.put(newKey.toString(), a); 
		}

		//newKey.set(shortEMA+""+longEMA+""+nVal+""+posIndex+":"+ticker); 

		if(entrySignal[trueIndex]){
			enterPosition(shortEMA, longEMA, nVal, trueIndex, context);
			return;
		}

		if(exitSignal[trueIndex]){
			exitPosition(shortEMA, longEMA, nVal, trueIndex, context);
			return; 
		}

		//	IF thisSpread is positive then a potential entry signal has occurred
		if(thisSpread > 0){

			// If a position is already entered then return
			//		can't enter an already entered position
			// 		Check if stop condition met;
			if(posEntered[trueIndex]){
				if(prices[low] < stopPrice[trueIndex]){
					exitOnStop(shortEMA, longEMA, nVal, trueIndex, context);
				}
				return;
			}

			// If prevSpread is <= 0 then an entry signal has occurred
			if(prevSpread <= 0){
				entrySignal[trueIndex] = true; 
				return; 
			}
		}
		//	IF thisSpread is negative then a potential exit signal has occurred
		if(thisSpread < 0){

			// If a position is not entered then there is no position to close
			//		can't exit a position that doesn't exist

			if(!posEntered[trueIndex]){
				return; 
			}

			// Check if stop condition met before signal given at closing price
			if(posEntered[trueIndex] && prices[low] < stopPrice[trueIndex]){
				exitOnStop(shortEMA, longEMA, nVal, trueIndex, context);
				return; 
			}

			// If prevSpread >= 0 then exit signal has occurred
			if(prevSpread >= 0){
				exitSignal[trueIndex] = true; 
				return; 
			}
		}

		// DEFAULT if thisSpread = 0 it is neither an entry or exit signal - do nothing

	}

	public void enterPosition(int shortEMA, int longEMA, int nVal, int posIndex, Context context){
		posEntered[posIndex] = true;
		entrySignal[posIndex] = false; 
		entryPrice[posIndex] = prices[open]; 
		shares[posIndex] = (int)(startCapital[posIndex] / entryPrice[posIndex]);
		entryCapital[posIndex] = (double)shares[posIndex] * entryPrice[posIndex]; 
		stopPrice[posIndex] = entryPrice[posIndex] - nVals[nVal];

		//		setValout("BUY", nVal, posIndex);
		//		
		//		ArrayList<String> a = results.get(newKey.toString());
		//		a.add(valout.toString()); 
		//		results.put(newKey.toString(), a); 
		lineBuilder[posIndex] = date+"\t"+startCapital[posIndex]+"\t"+entryPrice[posIndex]+"\t"+shares[posIndex]+"\t"+
				nVals[nVal]+"\t"+stopPrice[posIndex]+"\t";

		//		try {
		//			context.write(newKey, valout);
		//		} catch (IOException | InterruptedException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}

	}

	public void exitPosition(int shortEMA, int longEMA, int nVal, int posIndex, Context context){
		posEntered[posIndex] = false; 
		exitSignal[posIndex] = false; 
		exitPrice[posIndex] = prices[open];
		exitCapital[posIndex] = exitPrice[posIndex] * (double)shares[posIndex];
		realizedGains[posIndex] = exitCapital[posIndex] - entryCapital[posIndex]; 
		startCapital[posIndex] = startCapital[posIndex] + realizedGains[posIndex]; 

		double percentGain = ((exitCapital[posIndex]/entryCapital[posIndex]) - 1.0) * 100.00;

		lineBuilder[posIndex] += ("SELL\t"+date+"\t"+exitPrice[posIndex]+"\t"+realizedGains[posIndex]+"\t"+percentGain+"\t"+startCapital[posIndex]);

		//		setValout("SELL", nVal, posIndex);
		ArrayList<String> a = results.get(newKey.toString());
		//		a.add(valout.toString()); 
		a.add(lineBuilder[posIndex]);
		results.put(newKey.toString(), a); 
		lineBuilder[posIndex] = "";
		realizedGains[posIndex] = 0; 
		exitPrice[posIndex] = 0;
		exitCapital[posIndex] = 0; 
		stopPrice[posIndex] = 0; 
		shares[posIndex] = 0; 
		//		try {
		//			context.write(newKey, valout);
		//		} catch (IOException | InterruptedException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}

	public void exitOnStop(int shortEMA, int longEMA, int nVal, int posIndex, Context context){
		posEntered[posIndex] = false; 
		exitSignal[posIndex] = false; 

		if(prices[open] < stopPrice[posIndex]){
			exitPrice[posIndex] = prices[open];
			exitCapital[posIndex] = exitPrice[posIndex] * (double)shares[posIndex];
		}else{
			exitPrice[posIndex] = stopPrice[posIndex];
			exitCapital[posIndex] = exitPrice[posIndex] * (double)shares[posIndex];
		}
		realizedGains[posIndex] = exitCapital[posIndex] - entryCapital[posIndex]; 
		startCapital[posIndex] = startCapital[posIndex] + realizedGains[posIndex]; 

		double percentGain = ((exitPrice[posIndex]/entryPrice[posIndex]) - 1.0) * 100.00;

		lineBuilder[posIndex] += ("STOP\t"+date+"\t"+exitPrice[posIndex]+"\t"+realizedGains[posIndex]+"\t"+percentGain+"\t"+startCapital[posIndex]);

		//		setValout("STOP", nVal, posIndex);
		ArrayList<String> a = results.get(newKey.toString());
		//		a.add(valout.toString()); 
		a.add(lineBuilder[posIndex]);
		lineBuilder[posIndex] = ""; 
		results.put(newKey.toString(), a); 
		realizedGains[posIndex] = 0; 
		exitPrice[posIndex] = 0;
		exitCapital[posIndex] = 0; 
		stopPrice[posIndex] = 0; 
		shares[posIndex] = 0; 
		//		try {
		//			context.write(newKey, valout);
		//		} catch (IOException | InterruptedException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}

	public void setValout(String action, int nVal, int posIndex){
		valout.set(date+"\t"+action+"\t"+nVals[nVal]+"\t"+entryPrice[posIndex]+"\t"+exitPrice[posIndex]+"\t"+
				shares[posIndex]+"\t"+stopPrice[posIndex]+"\t"+startCapital[posIndex]+"\t"+entryCapital[posIndex]+"\t"+
				exitCapital[posIndex]+"\t"+realizedGains[posIndex]);
	}

	//	private String date;
	//	private String ticker; 
	//	private double[] prices = new double[pricesSize]; 
	//	private double[] thisEMA = new double[emaSize]; 
	//	private double[] prevEMA = new double[emaSize];
	//	private double[] nVals = new double[nSize];
	//	private double[] entryPrice = new double[posSize]; 
	//	private double[] exitPrice = new double[posSize]; 
	//	private int[] shares = new int[posSize];
	//	private double[] stopPrice = new double[posSize]; 
	//	private double[] startCapital = new double[posSize]; 
	//	private double[] entryCapital = new double[posSize]; 
	//	private double[] exitCapital = new double[posSize];
	//	private double[] realizedGains = new double[posSize]; 
}









//private DoubleWritable this12 = new DoubleWritable(0); 
//private DoubleWritable this26 = new DoubleWritable(0);
//private DoubleWritable this50 = new DoubleWritable(0); 
//private DoubleWritable this75 = new DoubleWritable(0);
//private DoubleWritable this150 = new DoubleWritable(0); 
//private DoubleWritable this200 = new DoubleWritable(0); 
//
//private DoubleWritable prev12 = new DoubleWritable(0);
//private DoubleWritable prev26 = new DoubleWritable(0);
//private DoubleWritable prev50 = new DoubleWritable(0);
//private DoubleWritable prev75 = new DoubleWritable(0); 
//private DoubleWritable prev150 = new DoubleWritable(0);
//private DoubleWritable prev200 = new DoubleWritable(0);
//
//private DoubleWritable halfN = new DoubleWritable(0); 
//private DoubleWritable oneN = new DoubleWritable(0); 
//private DoubleWritable twoN = new DoubleWritable(0); 


//private DoubleWritable[] EMAs = new DoubleWritable[6]; 
//private final int index12 = 0; 
//private final int index26 = 1; 
//private final int index50 = 2;
//private final int index75 = 3;
//private final int index150 = 4;
//private final int index200 = 5; 

//private DoubleWritable[] nVals = new DoubleWritable[3]; 
//private final int halfN = 0; 
//private final int oneN = 1;
//private final int twoN = 2; 


//context.write(key, val.getDate()); 
//this12 = val.getEMA12();
//this26 = val.getEMA26();
//this50 = val.getEMA50();
//this75 = val.getEMA75();
//this150 = val.getEMA150();
//this200 = val.getEMA200(); 
//
//oneN = val.getnValue(); 
//halfN.set(oneN.get() / 2.0); 
//twoN.set(oneN.get() * 2.0); 
