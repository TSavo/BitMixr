package com.google.bitcoin.core;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bitcoin.store.WalletProtobufSerializer;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class WalletStorage {

	private static final Logger log = LoggerFactory.getLogger(WalletStorage.class);

	/**
	 * Uses protobuf serialization to save the wallet to the given file stream.
	 * To learn more about this file format, see
	 * {@link WalletProtobufSerializer}.
	 */
	public static void saveToStream(Wallet w, OutputStream f) throws IOException {
		w.lock.lock();
		try {
			new WalletProtobufSerializer().writeWallet(w, f);
		} finally {
			w.lock.unlock();
		}
	}

	private static void saveToFile(Wallet w, File temp, File destFile) throws IOException {
		FileOutputStream stream = null;
		try {
			stream = new FileOutputStream(temp);
			saveToStream(w, stream);
			// Attempt to force the bits to hit the disk. In reality the OS or
			// hard disk itself may still decide
			// to not write through to physical media for at least a few
			// seconds, but this is the best we can do.
			stream.flush();
			stream.getFD().sync();
			stream.close();
			stream = null;
			if (Utils.isWindows()) {
				// Work around an issue on Windows whereby you can't rename over
				// existing files.
				File canonical = destFile.getCanonicalFile();
				canonical.delete();
				if (temp.renameTo(canonical))
					return; // else fall through.
				throw new IOException("Failed to rename " + temp + " to " + canonical);
			} else if (!temp.renameTo(destFile)) {
				throw new IOException("Failed to rename " + temp + " to " + destFile);
			}
		} finally {
			if (stream != null) {
				stream.close();
			}
			if (temp.delete()) {
				log.warn("Deleted temp file after failed save.");
			}
		}
	}

	/**
	 * Uses protobuf serialization to save the wallet to the given file. To
	 * learn more about this file format, see {@link WalletProtobufSerializer}.
	 * Writes out first to a temporary file in the same directory and then
	 * renames once written.
	 */
	public static void saveToFile(Wallet w, File f) throws IOException {
		File directory = f.getAbsoluteFile().getParentFile();
		File temp = File.createTempFile("wallet.tempsave", null, directory);
		saveToFile(w, temp, f);
	}

	/**
	 * Returns a wallet deserialized from the given file.
	 */
	public static Wallet loadFromFile(File f) throws IOException {
		FileInputStream stream = new FileInputStream(f);
		try {
			return loadFromStream(stream);
		} finally {
			stream.close();
		}
	}

	/**
	 * Returns a wallet deserialized from the given input stream.
	 */
	public static Wallet loadFromStream(InputStream aStream) throws IOException {
		// Determine what kind of wallet stream this is: Java Serialization or
		// protobuf format.
		InputStream stream = new BufferedInputStream(aStream);
		stream.mark(100);
		boolean serialization = stream.read() == 0xac && stream.read() == 0xed;
		stream.reset();

		Wallet wallet;

		if (serialization) {
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(stream);
				wallet = (Wallet) ois.readObject();
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			} finally {
				if (ois != null)
					ois.close();
			}
		} else {
			wallet = new WalletProtobufSerializer().readWallet(stream);
		}

		if (!wallet.isConsistent()) {
			log.error("Loaded an inconsistent wallet");
		}
		return wallet;
	}

	// Auto-saving can be done on a background thread if the user wishes it,
	// this is to avoid stalling threads calling
	// into the wallet on serialization/disk access all the time which is
	// important in GUI apps where you don't want
	// the main thread to ever wait on disk (otherwise you lose a lot of
	// responsiveness). The primary case where it
	// can be a problem is during block chain syncup - the wallet has to be
	// saved after every block to record where
	// it got up to and for updating the transaction confidence data, which can
	// slow down block chain download a lot.
	// So this thread not only puts the work of saving onto a background thread
	// but also coalesces requests together.
	private static class AutosaveThread extends Thread {
		private static DelayQueue<AutosaveThread.WalletSaveRequest> walletRefs = new DelayQueue<WalletSaveRequest>();
		private static AutosaveThread globalThread;

		private AutosaveThread() {
			// Allow the JVM to shut down without waiting for this thread. Note
			// this means users could lose auto-saves
			// if they don't explicitly save the wallet before terminating!
			setDaemon(true);
			setName("Wallet auto save thread");
			setPriority(Thread.MIN_PRIORITY); // Avoid competing with the UI.
		}

		/**
		 * Returns the global instance that services all wallets. It never shuts
		 * down.
		 */
		public static void maybeStart() {
			if (walletRefs.size() == 0)
				return;

			synchronized (AutosaveThread.class) {
				if (globalThread == null) {
					globalThread = new AutosaveThread();
					globalThread.start();
				}
			}
		}

		/**
		 * Called by a wallet when it's become dirty (changed). Will start the
		 * background thread if needed.
		 */
		public static void registerForSave(Wallet wallet, long delayMsec, File aSaveFile) {
			walletRefs.add(new WalletSaveRequest(wallet, delayMsec, aSaveFile));
			maybeStart();
		}

		@Override
		public void run() {
			log.info("Auto-save thread starting up");
			while (true) {
				try {
					WalletSaveRequest req = walletRefs.poll(5, TimeUnit.SECONDS);
					if (req == null) {
						if (walletRefs.size() == 0) {
							// No work to do for the given delay period, so
							// let's shut down and free up memory.
							// We'll get started up again if a wallet changes
							// once more.
							break;
						} else {
							// There's work but nothing to do just yet. Go back
							// to sleep and try again.
							continue;
						}
					}

					req.wallet.lock.lock();
					try {
						if (req.wallet.dirty) {
							try {
								req.save();
							} catch (IOException e) {
								throw new RuntimeException(e.getMessage(), e);
							}
							break;
						}
					} finally {
						req.wallet.lock.unlock();
					}
				} catch (InterruptedException e) {
					log.error("Auto-save thread interrupted during wait", e);
					break;
				}
			}
			log.info("Auto-save thread shutting down");
			synchronized (AutosaveThread.class) {
				Preconditions.checkState(globalThread == this); // There should
																// only be one
																// global
																// thread.
				globalThread = null;
			}
			// There's a possible shutdown race where work is added after we
			// decided to shutdown but before
			// we cleared globalThread.
			maybeStart();
		}

		private static class WalletSaveRequest implements Delayed {
			public final Wallet wallet;
			public final long startTimeMs, requestedDelayMs;
			private final File saveFile;

			public WalletSaveRequest(Wallet wallet, long requestedDelayMs, File aSaveFile) {
				this.startTimeMs = System.currentTimeMillis();
				this.requestedDelayMs = requestedDelayMs;
				this.wallet = wallet;
				saveFile = aSaveFile;
			}

			public void save() throws IOException {
				WalletStorage.saveToFile(wallet, saveFile);
			}

			@Override
			public long getDelay(TimeUnit timeUnit) {
				long delayRemainingMs = requestedDelayMs - (System.currentTimeMillis() - startTimeMs);
				return timeUnit.convert(delayRemainingMs, TimeUnit.MILLISECONDS);
			}

			@Override
			public int compareTo(Delayed delayed) {
				if (delayed == this)
					return 0;
				long delta = getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS);
				return (delta > 0 ? 1 : (delta < 0 ? -1 : 0));
			}

			@Override
			public boolean equals(Object obj) {
				if (!(obj instanceof WalletSaveRequest))
					return false;
				WalletSaveRequest w = (WalletSaveRequest) obj;
				return w.startTimeMs == startTimeMs && w.requestedDelayMs == requestedDelayMs && w.wallet == wallet;
			}

			@Override
			public int hashCode() {
				return Objects.hashCode(wallet, startTimeMs, requestedDelayMs);
			}

			public File getSaveFile() {
				return saveFile;
			}
		}
	}

//	/** Returns true if the auto-save thread should abort */
//	private boolean autoSave() {
//		lock.lock();
//		final Sha256Hash lastBlockSeenHash = this.lastBlockSeenHash;
//		final AutosaveEventListener autosaveEventListener = this.autosaveEventListener;
//		final File autosaveToFile = this.autosaveToFile;
//		lock.unlock();
//		try {
//			log.info("Auto-saving wallet, last seen block is {}", lastBlockSeenHash);
//			File directory = autosaveToFile.getAbsoluteFile().getParentFile();
//			File temp = File.createTempFile("wallet", null, directory);
//			if (autosaveEventListener != null)
//				autosaveEventListener.onBeforeAutoSave(temp);
//			// This will clear the dirty flag.
//			saveToFile(temp, autosaveToFile);
//			if (autosaveEventListener != null)
//				autosaveEventListener.onAfterAutoSave(autosaveToFile);
//		} catch (Exception e) {
//			if (autosaveEventListener != null && autosaveEventListener.caughtException(e))
//				return true;
//			else
//				throw new RuntimeException(e);
//		}
//		return false;
//	}
//
//	/**
//	 * Implementors can handle exceptions thrown during wallet auto-save, and to
//	 * do pre/post treatment of the wallet.
//	 */
//	public interface AutosaveEventListener {
//		/**
//		 * Called on the auto-save thread if an exception is caught whilst
//		 * saving the wallet.
//		 * 
//		 * @return if true, terminates the auto-save thread. Otherwise sleeps
//		 *         and then tries again.
//		 */
//		public boolean caughtException(Throwable t);
//
//		/**
//		 * Called on the auto-save thread when a new temporary file is created
//		 * but before the wallet data is saved to it. If you want to do
//		 * something here like adjust permissions, go ahead and do so. The
//		 * wallet is locked whilst this method is run.
//		 */
//		public void onBeforeAutoSave(File tempFile);
//
//		/**
//		 * Called on the auto-save thread after the newly created temporary file
//		 * has been filled with data and renamed. The wallet is locked whilst
//		 * this method is run.
//		 */
//		public void onAfterAutoSave(File newlySavedFile);
//	}
//
//	/**
//	 * <p>
//	 * Sets up the wallet to auto-save itself to the given file, using temp
//	 * files with atomic renames to ensure consistency. After connecting to a
//	 * file, you no longer need to save the wallet manually, it will do it
//	 * whenever necessary. Protocol buffer serialization will be used.
//	 * </p>
//	 * 
//	 * <p>
//	 * If delayTime is set, a background thread will be created and the wallet
//	 * will only be saved to disk every so many time units. If no changes have
//	 * occurred for the given time period, nothing will be written. In this way
//	 * disk IO can be rate limited. It's a good idea to set this as otherwise
//	 * the wallet can change very frequently, eg if there are a lot of
//	 * transactions in it or during block sync, and there will be a lot of
//	 * redundant writes. Note that when a new key is added, that always results
//	 * in an immediate save regardless of delayTime. <b>You should still save
//	 * the wallet manually when your program is about to shut down as the JVM
//	 * will not wait for the background thread.</b>
//	 * </p>
//	 * 
//	 * <p>
//	 * An event listener can be provided. If a delay >0 was specified, it will
//	 * be called on a background thread with the wallet locked when an auto-save
//	 * occurs. If delay is zero or you do something that always triggers an
//	 * immediate save, like adding a key, the event listener will be invoked on
//	 * the calling threads.
//	 * </p>
//	 * 
//	 * @param f
//	 *            The destination file to save to.
//	 * @param delayTime
//	 *            How many time units to wait until saving the wallet on a
//	 *            background thread.
//	 * @param timeUnit
//	 *            the unit of measurement for delayTime.
//	 * @param eventListener
//	 *            callback to be informed when the auto-save thread does things,
//	 *            or null
//	 */
//	public void autosaveToFile(File f, long delayTime, TimeUnit timeUnit, AutosaveEventListener eventListener) {
//		lock.lock();
//		try {
//			Preconditions.checkArgument(delayTime >= 0);
//			autosaveToFile = Preconditions.checkNotNull(f);
//			if (delayTime > 0) {
//				autosaveEventListener = eventListener;
//				autosaveDelayMs = TimeUnit.MILLISECONDS.convert(delayTime, timeUnit);
//			}
//		} finally {
//			lock.unlock();
//		}
//	}
//
//	private void queueAutoSave() {
//		lock.lock();
//		try {
//			if (this.autosaveToFile == null)
//				return;
//			if (autosaveDelayMs == 0) {
//				// No delay time was specified, so save now.
//				try {
//					saveToFile(autosaveToFile);
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//			} else {
//				// If we need to, tell the auto save thread to wake us up. This
//				// will start the background thread if one
//				// doesn't already exist. It will wake up once the delay expires
//				// and call autoSave().
//				// The background thread is shared between all wallets.
//				if (!dirty) {
//					dirty = true;
//					AutosaveThread.registerForSave(this, autosaveDelayMs);
//				}
//			}
//		} finally {
//			lock.unlock();
//		}
//	}

}
