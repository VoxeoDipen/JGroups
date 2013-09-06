
package org.jgroups;


import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * A view is a local representation of the current membership of a group. Only one view is installed
 * in a channel at a time. Views contain the address of its creator, an ID and a list of member
 * addresses. These addresses are ordered, and the first address is always the coordinator of the
 * view. This way, each member of the group knows who the new coordinator will be if the current one
 * crashes or leaves the group. The views are sent between members using the VIEW_CHANGE event
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class View implements Comparable<View>, Streamable, Iterable<Address> {

   /**
    * A view is uniquely identified by its ViewID. The view id contains the creator address and a
    * Lamport time. The Lamport time is the highest timestamp seen or sent from a view. if a view
    * change comes in with a lower Lamport time, the event is discarded.
    */
    protected ViewId vid;

   /**
    * A list containing all the members of the view.This list is always ordered, with the
    * coordinator being the first member. the second member will be the new coordinator if the
    * current one disappears or leaves the group.
    */
    protected List<Address> members;



    /**
     * Creates an empty view, should not be used, only used by (de-)serialization
     */
    public View() {
    }


    /**
     * Creates a new view
     *
     * @param vid     The view id of this view (can not be null)
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(ViewId vid, List<Address> members) {
        this.vid=vid;
        this.members=Collections.unmodifiableList(members);
    }

    /**
     * Creates a new view
     *
     * @param creator The creator of this view (can not be null)
     * @param id      The lamport timestamp of this view
     * @param members Contains a list of all the members in the view, can be empty but not null.
     */
    public View(Address creator, long id, List<Address> members) {
        this(new ViewId(creator, id), members);
    }

    public static View create(Address coord, long id, Address ... members) {
        return new View(coord, id, Arrays.asList(members));
    }

    /**
     * Returns the view ID of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the view ID of this view
     */
    public ViewId getVid()    {return vid;}
    public ViewId getViewId() {return vid;}

    /**
     * Returns the creator of this view
     * if this view was created with the empty constructur, null will be returned
     *
     * @return the creator of this view in form of an Address object
     */
    public Address getCreator() {
        return vid.getCreator();
    }

    /**
     * Returns the member list
     * @return an unmodifiable list of members
     */
    public List<Address> getMembers() {
        return members;
    }

    /**
     * Returns true, if this view contains a certain member
     *
     * @param mbr - the address of the member,
     * @return true if this view contains the member, false if it doesn't
     *         if the argument mbr is null, this operation returns false
     */
    public boolean containsMember(Address mbr) {
        return mbr != null && members.contains(mbr);
    }


    public int compareTo(View o) {
        return vid.compareTo(o.vid);
    }

    public boolean equals(Object obj) {
        return obj instanceof View && (this == obj || compareTo((View)obj) == 0);
    }


    public int hashCode() {
        return vid.hashCode();
    }

    /**
     * Returns the number of members in this view
     *
     * @return the number of members in this view 0..n
     */
    public int size() {
        return members.size();
    }


    public View copy() {
        // to avoid cascading refs (UnmodifiableList keeps a ref to the wrapped list)
        return new View(vid.copy(), new ArrayList<Address>(members));
    }


    public String toString() {
        StringBuilder sb=new StringBuilder(64);
        sb.append(vid).append(" ");
        if(members != null)
            sb.append("[").append(Util.printListWithDelimiter(members, ", ", Util.MAX_LIST_PRINT_SIZE)).append("]");
        return sb.toString();
    }


    public void writeTo(DataOutput out) throws Exception {
        vid.writeTo(out);
        Util.writeAddresses(members, out);
    }

    @SuppressWarnings("unchecked") 
    public void readFrom(DataInput in) throws Exception {
        vid=new ViewId();
        vid.readFrom(in);
        members=Collections.unmodifiableList((List<? extends Address>)Util.readAddresses(in, ArrayList.class));
    }

    public int serializedSize() {
        return (int)(vid.serializedSize() + Util.size(members));
    }

    /**
     * Returns a list of members which left from view one to two
     * @param one
     * @param two
     * @return
     */
    public static List<Address> leftMembers(View one, View two) {
        if(one == null || two == null)
            return null;
        List<Address> retval=new ArrayList<Address>(one.getMembers());
        retval.removeAll(two.getMembers());
        return retval;
    }

    /**
     * Returns the difference between 2 views from and to. It is assumed that view 'from' is logically prior to view 'to'.
     * @param from The first view
     * @param to The second view
     * @return an array of 2 Address arrays: index 0 has the addresses of the joined member, index 1 those of the left members
     */
    public static Address[][] diff(final View from, final View to) {
        if(to == null)
            throw new IllegalArgumentException("the second view cannot be null");
        if(from == null) {
            Address[] joined=new Address[to.size()];
            int index=0;
            for(Address addr: to.getMembers())
                joined[index++]=addr;
            return new Address[][]{joined,{}};
        }

        Address[] joined=null, left=null;
        int num_joiners=0, num_left=0;

        // determin joiners
        for(Address addr: to)
            if(!from.containsMember(addr))
                num_joiners++;
        if(num_joiners > 0) {
            joined=new Address[num_joiners];
            int index=0;
            for(Address addr: to)
                if(!from.containsMember(addr))
                    joined[index++]=addr;
        }

        // determin leavers
        for(Address addr: from)
            if(!to.containsMember(addr))
                num_left++;
        if(num_left > 0) {
            left=new Address[num_left];
            int index=0;
            for(Address addr: from)
                if(!to.containsMember(addr))
                    left[index++]=addr;
        }

        return new Address[][]{joined != null? joined : new Address[]{}, left != null? left : new Address[]{}};
    }

    public Iterator<Address> iterator() {
        return members.iterator();
    }
}
