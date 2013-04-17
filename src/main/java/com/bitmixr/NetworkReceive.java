package com.bitmixr;

import java.math.BigInteger;

public class NetworkReceive {

	public String payerId;
	public String payeeId;
	public BigInteger amountSpent;
	public BigInteger tip;

	public String getPayerId() {
		return payerId;
	}

	public void setPayerId(String payerId) {
		this.payerId = payerId;
	}

	public String getPayeeId() {
		return payeeId;
	}

	public void setPayeeId(String payeeId) {
		this.payeeId = payeeId;
	}

	public BigInteger getAmountSpent() {
		return amountSpent;
	}

	public void setAmountSpent(BigInteger amountSpent) {
		this.amountSpent = amountSpent;
	}

	public NetworkReceive(String payerId, String payeeId, BigInteger amountSpent, BigInteger aTip) {
		super();
		this.payerId = payerId;
		this.payeeId = payeeId;
		this.amountSpent = amountSpent;
		tip = aTip;
	}

	public BigInteger getTip() {
		return tip;
	}

	public void setTip(BigInteger tip) {
		this.tip = tip;
	}

}
