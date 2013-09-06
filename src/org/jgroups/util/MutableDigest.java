package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.View;

/**
 * A mutable version of Digest. Has a fixed size (that of the view), but individual elements can be changed.
 * This class is not synchronized
 * @author Bela Ban
 */
public class MutableDigest extends Digest {
    public MutableDigest() { // for externalization
        super();
    }


    public MutableDigest(View view, long[] seqnos) {
        super(view,seqnos);
    }

    public MutableDigest(Digest digest) {
        super(digest);
    }

    public MutableDigest(final View view) {
        if(view == null) throw new IllegalArgumentException("view is null");
        this.view=view;
        seqnos=new long[view().size() * 2];
        checkPostcondition();
    }

    public MutableDigest set(Address member, long highest_delivered_seqno, long highest_received_seqno) {
        if(member == null)
            return this;
        int index=find(member);
        if(index >= 0) {
            seqnos[index * 2]=highest_delivered_seqno;
            seqnos[index * 2 +1]=highest_received_seqno;
        }
        return this;
    }



    public MutableDigest set(Digest digest) {
        if(digest == null)
            return this;
        for(Entry entry: digest)
            set(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
        return this;
    }



    /**
     * Adds a digest to this digest. For each sender in the other digest, the merge() method will be called.
     */
    public MutableDigest merge(Digest digest) {
        if(digest == null)
            return this;
        for(Entry entry: digest)
            merge(entry.getMember(), entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno());
        return this;
    }


    /**
     * Similar to set(), but if the sender already exists, its seqnos will be modified (no new entry) as follows:
     * <ol>
     * <li>this.highest_delivered_seqno=max(this.highest_delivered_seqno, highest_delivered_seqno)
     * <li>this.highest_received_seqno=max(this.highest_received_seqno, highest_received_seqno)
     * </ol>
     */
    public MutableDigest merge(final Address member, final long highest_delivered_seqno, final long highest_received_seqno) {
        if(member == null)
            return this;
        long[] entry=get(member);
        long hd=entry == null? highest_delivered_seqno : Math.max(entry[0],highest_delivered_seqno);
        long hr=entry == null? highest_received_seqno  : Math.max(entry[1],highest_received_seqno);
        return set(member, hd, hr);
    }


}
