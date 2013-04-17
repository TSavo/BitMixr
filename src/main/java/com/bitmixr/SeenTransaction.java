package com.bitmixr;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionConfidence;

@Entity
public class SeenTransaction implements TransactionConfidence.Listener {

	@Id
	private String id = UUID.randomUUID().toString();
	String transactionHash;
	@ManyToOne
	@JoinColumn(name = "paymentId", nullable = false)
	Payment payment;
	@Temporal(value = TemporalType.TIMESTAMP)
	Date seenOn = new Date();
	BigInteger amount;
	public AtomicBoolean getDirty() {
		return dirty;
	}

	public void setDirty(AtomicBoolean dirty) {
		this.dirty = dirty;
	}

	transient AtomicBoolean dirty = new AtomicBoolean(false);

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTransactionHash() {
		return transactionHash;
	}

	public void setTransactionHash(String transactionHash) {
		this.transactionHash = transactionHash;
	}

	public Payment getPayment() {
		return payment;
	}

	public void setPayment(Payment payment) {
		this.payment = payment;
	}

	public Date getSeenOn() {
		return seenOn;
	}

	public void setSeenOn(Date seenOn) {
		this.seenOn = seenOn;
	}

	public BigInteger getAmount() {
		return amount;
	}

	public void setAmount(BigInteger amount) {
		this.amount = amount;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(id).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != getClass()) {
			return false;
		}
		SeenTransaction rhs = (SeenTransaction) obj;
		return new EqualsBuilder().append(id, rhs.id).isEquals();
	}

	@Override
	public void onConfidenceChanged(Transaction tx) {
		if (tx.getConfidence().getConfidenceType() == TransactionConfidence.ConfidenceType.BUILDING) {
			tx.getConfidence().removeEventListener(this);
			getDirty().set(true);
		}
	}

}
