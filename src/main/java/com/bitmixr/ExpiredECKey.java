package com.bitmixr;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.bitcoin.core.ECKey;

@Entity
public class ExpiredECKey {

	@Id
	String id = UUID.randomUUID().toString();
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

	@Temporal(TemporalType.TIMESTAMP)
	Date createdOn;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public byte[] getPrivateKey() {
		return privateKey;
	}

	public void setPrivateKey(byte[] privateKey) {
		this.privateKey = privateKey;
	}

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

	public ExpiredECKey(Payment aPayment) {
		privateKey = Arrays.copyOf(aPayment.getPrivateKey(), aPayment.getPrivateKey().length);
		publicKey = Arrays.copyOf(aPayment.getPublicKey(), aPayment.getPublicKey().length);
		createdOn = aPayment.getCreatedOn();
	}

	public ExpiredECKey(ECKey aKey) {
		privateKey = Arrays.copyOf(aKey.getPrivKeyBytes(), aKey.getPrivKeyBytes().length);
		publicKey = Arrays.copyOf(aKey.getPubKey(), aKey.getPubKey().length);
		createdOn = new Date(aKey.getCreationTimeSeconds() * 1000);
	}

	public ECKey getECKey() {
		ECKey key = new ECKey(Arrays.copyOf(privateKey, privateKey.length), Arrays.copyOf(publicKey, publicKey.length));
		key.setCreationTimeSeconds(createdOn.getTime() / 1000);
		return key;
	}

}
