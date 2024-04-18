package edu.rice.comp322.boruvka.parallel;

import edu.rice.comp322.AbstractBoruvka;
import edu.rice.comp322.boruvka.BoruvkaFactory;
import edu.rice.comp322.boruvka.Edge;
import edu.rice.comp322.boruvka.Loader;
import edu.rice.hj.api.SuspendableException;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class must be modified.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ParBoruvka extends AbstractBoruvka implements BoruvkaFactory<ParComponent, ParEdge> {

    protected final ConcurrentLinkedQueue<ParComponent> nodesLoaded = new ConcurrentLinkedQueue<>();
    protected ParComponent ans =  null;

    public ParBoruvka() {
        super();
    }

    @Override
    public boolean usesHjLib() {
        return true;
    }

    @Override
    public void initialize(final String[] args) {
        Loader.parseArgs(args);
    }

    @Override
    public void preIteration(final int iterationIndex) {
        // Exclude reading file input from timing measurement
        nodesLoaded.clear();
        Loader.read(this, nodesLoaded);

        totalEdges = 0;
        totalWeight = 0;
    }

    @Override
    public void runIteration(int nthreads) throws SuspendableException {
        computeBoruvka(nodesLoaded, nthreads);
    }

    private void computeBoruvka(final Queue<ParComponent> nodesLoaded, int nthreads) {

        //initialize array of threads to iterate through
        final Thread[] threads = new Thread[nthreads];

        for (int i = 0; i < nthreads; i++) {
            //create n threads each working on a component if available
            threads[i] = new Thread(()-> {

                ParComponent loopNode = null;
                while ((loopNode = nodesLoaded.poll()) != null) {

                    if (!loopNode.lock.tryLock()) {
                        continue;
                    }
                    if (loopNode.isDead) {
                        loopNode.lock.unlock();
                        continue;
                    }
                    final Edge<ParComponent> minEdge = loopNode.getMinEdge();

                    if (minEdge == null) {
                        ans = loopNode;
                        break;
                    }

                    final ParComponent adjNode = minEdge.getOther(loopNode);

                    if (!(adjNode.lock.tryLock())) {
                        loopNode.lock.unlock();
                        nodesLoaded.add(loopNode);
                        continue;
                    }

                    if (adjNode.isDead) {
                        adjNode.lock.unlock();
                        loopNode.lock.unlock();
                        nodesLoaded.add(loopNode);
                        continue;
                    }
                    adjNode.isDead = true;
                    loopNode.merge(adjNode, minEdge.weight());
                    loopNode.lock.unlock();
                    adjNode.lock.unlock();
                    nodesLoaded.add(loopNode);

                }

            });
            threads[i].start();


        }

        for (int i = 0; i < nthreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                System.out.println("Interrupted");
            }
        }
        // don't set ans until all threads have been joined
        if (ans != null) {
            totalEdges = ans.totalEdges();
            totalWeight = ans.totalWeight();

        }



    }

    @Override
    public ParComponent newComponent(final int nodeId) {
        return new ParComponent(nodeId);
    }

    @Override
    public ParEdge newEdge(final ParComponent from, final ParComponent to, final double weight) {
        return new ParEdge(from, to, weight);
    }
}


