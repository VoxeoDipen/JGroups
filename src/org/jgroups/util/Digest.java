package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.annotations.Immutable;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import static java.lang.Math.max;


/**
 * A message digest containing - for each member - the highest seqno delivered (hd) and the highest seqno received (hr).
 * The seqnos are stored according to the order of the members in the associated view, ie. seqnos[0] is the hd for
 * member at index 0, seqnos[1] is the hr for the same member, seqnos[2] is the hd for member at index 1 and so on.<p/>
 * Field 'view' is usually a View referring to an existing view. Thus, many digests can refer to the same view, which
 * is memory efficient. When unmarshalled, 'view' can also be a ViewdId, used to check if the caller's view-id and the
 * digest's view-id match. If so, usually {@link #view(org.jgroups.View)} is called to set the view to a real View.
 * @author Bela Ban
 */
public class Digest implements Streamable, Iterable<Digest.Entry> {

    /** Contains a ViewId or a View. Done to save memory and simulate a C union. Usually, the View is set
     * in the constructor. However, when received over the network and unmarshalled, a ViewId will be set.
     * However, it is expected that a View is set immediately after unmarshalling. So most of the time, view
     * should be of type View. Note that view must <em>never be null</em>. */
    protected Object    view;

    // Stores highest delivered and received seqnos. This array is double the size of members. We store HD-HR pairs,
    // so to get the HD seqno for member P at index i --> seqnos[i*2], to get the HR --> seqnos[i*2 +1]
    protected long[]    seqnos;



    /** Used for serialization */
    public Digest() {
    }

    public Digest(final View view, long[] seqnos) {
        if(view == null) throw new IllegalArgumentException("view is null");
        if(seqnos == null) throw new IllegalArgumentException("seqnos is null");
        this.view=view;
        this.seqnos=seqnos;
        checkPostcondition();
    }


    public Digest(Digest digest) {
        if(digest == null)
            return;
        int size=digest.capacity();
        seqnos=new long[size * 2];
        System.arraycopy(digest.seqnos,0,seqnos,0,size * 2);
        this.view=digest.view;
        checkPostcondition();
    }


    public int capacity() {
        return seqnos != null? seqnos.length /2 : 0;
    }

    public View view() {
        return view instanceof View? (View)view : null;
    }

    public Digest view(View view) {
        if(view == null || view() != null) // the view can only be set once
            return this;
        ViewId view_id=viewId();
        if(view_id != null && !view_id.equals(view.getViewId()))
            return this;
        this.view=view;
        return this;
    }

    public ViewId viewId() {
        return view instanceof ViewId? (ViewId)view : view instanceof View? ((View)view).getViewId() : null;
    }


    public boolean contains(Address member) {
        return view().containsMember(member);
    }

    public boolean containsAll(Address ... members) {
        for(Address member: members)
            if(!contains(member))
                return false;
        return true;
    }


    /** 2 digests are equal if their view-ids match and all highest-delivered and highest-received seqnos match */
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        Digest other=(Digest)obj;
        return viewId().equals(other.viewId()) && Arrays.equals(seqnos,other.seqnos);
    }

    /**
     * Returns the highest delivered and received seqnos associated with a member.
     * @param member
     * @return An array of 2 elements: highest_delivered and highest_received seqnos
     */
    public long[] get(Address member) {
        int index=find(member);
        if(index < 0)
            return null;
        return new long[]{seqnos[index * 2], seqnos[index * 2 +1]};
    }


    public Iterator<Entry> iterator() {
        return new MyIterator();
    }


    public Digest copy() {
        return new Digest(view(), Arrays.copyOf(seqnos, seqnos.length));
    }

    // we're only marshalling the ViewId and the seqnos
    public void writeTo(DataOutput out) throws Exception {
        Util.writeViewId(viewId(), out);
        out.writeShort(capacity());
        for(int i=0; i < capacity(); i++)
            Util.writeLongSequence(seqnos[i * 2], seqnos[i * 2 +1], out);
    }


    public void readFrom(DataInput in) throws Exception {
        this.view=Util.readViewId(in);
        short size=in.readShort();
        this.seqnos=new long[size*2];
        for(int i=0; i < size; i++) {
            long[] tmp=Util.readLongSequence(in);
            seqnos[i * 2]=tmp[0];
            seqnos[i * 2 +1]=tmp[1];
        }
    }


    public long serializedSize() {
        long retval=Global.SHORT_SIZE + Util.size(viewId()); // number of elements in 'senders'
        for(int i=0; i < capacity(); i++)
            retval+=Util.size(seqnos[i*2], seqnos[i*2+1]);
        return retval;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder(viewId() + ": ");
        boolean first=true;
        if(capacity() == 0 || view() == null) return view != null? viewId().toString() : "[]";

        int count=0, capacity=capacity();
        for(Entry entry: this) {
            Address key=entry.getMember();
            if(!first)
                sb.append(", ");
            else
                first=false;
            sb.append(key).append(": ").append('[').append(entry.getHighestDeliveredSeqno());
            if(entry.getHighestReceivedSeqno() >= 0)
                sb.append(" (").append(entry.getHighestReceivedSeqno()).append(")");
            sb.append("]");
            if(Util.MAX_LIST_PRINT_SIZE > 0 && ++count >= Util.MAX_LIST_PRINT_SIZE) {
                if(capacity > count)
                    sb.append(", ...");
                break;
            }
        }
        return sb.toString();
    }

    public String toStringSorted() {
        return toStringSorted(true);
    }

    public String toStringSorted(boolean print_highest_received) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        if(capacity() == 0)  return view != null? viewId().toString() : "[]";

        TreeMap<Address,long[]> copy=new TreeMap<Address,long[]>();
        for(Entry entry: this) {
            Address addr=entry.getMember();
            long[] tmp={entry.getHighestDeliveredSeqno(), entry.getHighestReceivedSeqno()};
            copy.put(addr, tmp);
        }

        int count=0, size=copy.size();
        for(Map.Entry<Address,long[]> entry: copy.entrySet()) {
            Address key=entry.getKey();
            long[] val=entry.getValue();
            if(!first)
                sb.append(", ");
            else
                first=false;
            sb.append(key).append(": ").append('[').append(val[0]);
            if(print_highest_received)
                sb.append(" (").append(val[1]).append(")");
            sb.append("]");
            if(Util.MAX_LIST_PRINT_SIZE > 0 && ++count >= Util.MAX_LIST_PRINT_SIZE) {
                if(size > count)
                    sb.append(", ...");
                break;
            }
        }
        return sb.toString();
    }


    public String printHighestDeliveredSeqnos() {
        return toStringSorted(false);
    }




    protected int find(Address member) {
        return view().getMembers().indexOf(member);
    }


    /** view.size() == capacity() */
    protected void checkPostcondition() {
        int size=view().size();
        if(size*2 != seqnos.length)
            throw new IllegalArgumentException("seqnos.length (" + seqnos.length + ") is not twice the view size (" + size + ")");
    }


    protected class MyIterator implements Iterator<Entry> {
        protected int index;

        public boolean hasNext() {
            return index < capacity();
        }

        public Entry next() {
            if(index >= capacity())
                throw new NoSuchElementException("index=" + index + ", capacity=" + capacity());

            Entry entry=new Entry(view().getMembers().get(index), seqnos[index * 2], seqnos[index * 2 +1]);
            index++;
            return entry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


    /** Keeps track of one members plus its highest delivered and received seqnos */
    @Immutable
    public static class Entry {
        protected final Address member;
        protected final long    highest_delivered;
        protected final long    highest_received;

        public Entry(Address member, long highest_delivered, long highest_received) {
            this.member=member;
            this.highest_delivered=highest_delivered;
            this.highest_received=highest_received;
        }

        public Address getMember()                {return member;}
        public long    getHighestDeliveredSeqno() {return highest_delivered;}
        public long    getHighestReceivedSeqno()  {return highest_received;}
        public long    getHighest()               {return max(highest_delivered, highest_received);}

        public String toString() {
            return member + ": [" + highest_delivered + " (" + highest_received + ")]";
        }
    }


}
