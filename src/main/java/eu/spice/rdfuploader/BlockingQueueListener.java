
package eu.spice.rdfuploader;

import java.util.concurrent.LinkedBlockingQueue;

public class BlockingQueueListener<E> extends LinkedBlockingQueue<E> {

	private LinkedBlockingQueue<E> sideQueue;
	private static final long serialVersionUID = 1L;

	public BlockingQueueListener(int size) {
		super(size);
		sideQueue = new LinkedBlockingQueue<>(size);
	}

	public E take() throws InterruptedException {
		E result = super.take();
		sideQueue.put(result);
		return result;
	}

	public LinkedBlockingQueue<E> getSideQueue() {
		return sideQueue;
	}

}
