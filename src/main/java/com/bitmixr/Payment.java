package com.bitmixr;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToMany;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.bitcoin.core.ECKey;

@JsonInclude(Include.NON_NULL)
@Entity
public class Payment implements Serializable {

	@Id
	private String id = UUID.randomUUID().toString();
	private String sourceAddress = null;
	private String destinationAddress = null;
	private BigInteger recievedAmount = BigInteger.ZERO;
	private BigInteger sentAmount = BigInteger.ZERO;
	private BigInteger totalToSend = BigInteger.ZERO;
	@JsonIgnore
	private BigInteger spentAmount = BigInteger.ZERO;
	@JsonIgnore
	private BigInteger receivedTip = BigInteger.ZERO;
	@JsonIgnore
	private BigInteger spentTip = BigInteger.ZERO;
	@JsonIgnore
	@Temporal(TemporalType.TIMESTAMP)
	private Date createdOn = new Date();
	@JsonIgnore
	@Temporal(TemporalType.TIMESTAMP)
	private Date updatedOn = new Date();
	@JsonIgnore
	@Temporal(TemporalType.TIMESTAMP)
	private Date paidOn;
	
	private boolean visible = true;

	@JsonIgnore
	@OneToMany(cascade = CascadeType.ALL, mappedBy = "payment", fetch = FetchType.EAGER, orphanRemoval = true)
	Set<SeenTransaction> seenTransactions = new HashSet<>();

	@JsonIgnore
	@Lob
	@Basic(fetch = FetchType.EAGER)
	@Column(name = "privateKey", columnDefinition = "LONGBLOB")
	byte[] privateKey;

	@JsonIgnore
	@Lob
	@Basic(fetch = FetchType.EAGER)
	@Column(name = "publicKey", columnDefinition = "LONGBLOB")
	byte[] publicKey;

	public BigInteger getTotalToSend() {
		return totalToSend;
	}

	public void setTotalToSend(BigInteger totalToSend) {
		this.totalToSend = totalToSend;
	}

	public Date getPaidOn() {
		return paidOn;
	}

	public void setPaidOn(Date paidOn) {
		this.paidOn = paidOn;
	}

	public Date getUpdatedOn() {
		return updatedOn;
	}

	public BigInteger getReceivedTip() {
		return receivedTip;
	}

	public void setReceivedTip(BigInteger receivedTip) {
		this.receivedTip = receivedTip;
	}

	public BigInteger getSpentTip() {
		return spentTip;
	}

	public void setSpentTip(BigInteger spentTip) {
		this.spentTip = spentTip;
	}

	public void setSentAmount(BigInteger sentAmount) {
		this.sentAmount = sentAmount;
	}

	public BigInteger getSentAmount() {
		return sentAmount;
	}

	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean isVisible) {
		this.visible = isVisible;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public BigInteger getRecievedAmount() {
		return recievedAmount;
	}

	public void setRecievedAmount(BigInteger amount) {
		this.recievedAmount = amount;
	}

	public String getSourceAddress() {
		return sourceAddress;
	}

	public void setSourceAddress(String address) {
		this.sourceAddress = address;
	}

	public String getDestinationAddress() {
		return destinationAddress;
	}

	public void setDestinationAddress(String destinationAddress) {
		this.destinationAddress = destinationAddress;
	}

	public void setECKey(ECKey aKey) {
		privateKey = Arrays.copyOf(aKey.getPrivKeyBytes(), aKey.getPrivKeyBytes().length);
		publicKey = Arrays.copyOf(aKey.getPubKey(), aKey.getPubKey().length);
		createdOn = new Date(aKey.getCreationTimeSeconds() * 1000);
	}

	@JsonIgnore
	public ECKey getECKey() {
		ECKey key = new ECKey(Arrays.copyOf(privateKey, privateKey.length), Arrays.copyOf(publicKey, publicKey.length));
		key.setCreationTimeSeconds(getCreatedOn().getTime() / 1000);
		return key;
	}

	@JsonIgnore
	public Set<SeenTransaction> getSeenTransactions() {
		return seenTransactions;
	}

	public void setSeenTransactions(Set<SeenTransaction> seenTransactions) {
		this.seenTransactions = seenTransactions;
	}

	@JsonIgnore
	public byte[] getPrivateKey() {
		return privateKey;
	}

	public void setPrivateKey(byte[] privateKey) {
		this.privateKey = privateKey;
	}

	@JsonIgnore
	public byte[] getPublicKey() {
		return publicKey;
	}

	public void setPublicKey(byte[] publicKey) {
		this.publicKey = publicKey;
	}

	public Date getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(Date createdOn) {
		this.createdOn = createdOn;
	}

	@JsonIgnore
	public BigInteger getSpentAmount() {
		return spentAmount;
	}

	public void setSpentAmount(BigInteger spentAmount) {
		this.spentAmount = spentAmount;
	}

	public void setUpdatedOn(Date date) {
		updatedOn = date;
	}
}
