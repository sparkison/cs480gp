/**
 * @author Shaun Parkison
 * Custom CompositeKey writable so we
 * can doubly sort the keys from map outputs
 * Will sort first based on stock, and second based on date
 */

package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	/**
	 * the ticker field
	 */
	Text ticker;

	/**
	 * the date field
	 */
	Text date;

	public CompositeKey() {}

	public CompositeKey(Text ticker, Text date) {
		this.ticker = ticker;
		this.date = date;
	}

	/**
	 * set the ticker Text
	 * @param ticker
	 */
	public void setTicker(Text t) {
		if (ticker == null)
			ticker = new Text();

		ticker.set(t);
	}

	/**
	 * set the date text
	 * @param date
	 */
	public void setDate(Text d) {
		if (date == null)
			date = new Text();

		date.set(d);
	}

	/**
	 * get the ticker field
	 * 
	 * @return the ticker field
	 */
	public Text getTicker() {
		return ticker;
	}

	/**
	 * get the date field
	 * 
	 * @return the date field
	 */
	public Text getDate() {
		return date;
	}

	public void write(DataOutput out) throws IOException {
		ticker.write(out);
		date.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		if (ticker == null)
			ticker = new Text();

		if (date == null)
			date = new Text();

		ticker.readFields(in);
		date.readFields(in);
	}

	public int compareTo(CompositeKey other) {
		int cmp = getTicker().compareTo(other.getTicker());
		if (cmp != 0) {
			return cmp;
		}
		return this.getDate().compareTo(other.getDate()); // reverse
	}

	public int hashCode() {
		return ticker.hashCode();
	}

	public boolean equals(Object o) {
		CompositeKey p = (CompositeKey) o;
		return ticker.equals(p.getTicker());
	}
	public String toString(){
		return ticker.toString()+":"+date.toString();
	}
}
