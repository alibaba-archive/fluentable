package com.aliyun.iotx.fluentable.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
public interface GenericQueue<E> extends Iterable<QueueElement<E>>{
    /**
     * Inserts the specified element into this queue if it is possible to do so
     * immediately without violating capacity restrictions, returning
     * {@code true} upon success and throwing an {@code IllegalStateException}
     * if no space is currently available.
     *
     * @param e the element to add
     * @return the sequence if the element was added to this queue, else {@code null}
     * @throws NullPointerException if the specified element is null and
     *         this queue does not permit null elements
     */
    Long push(E e);

    /**
     * Adds all of the elements in the specified collection to this collection
     * (optional operation).  The behavior of this operation is undefined if
     * the specified collection is modified while the operation is in progress.
     * (This implies that the behavior of this call is undefined if the
     * specified collection is this collection, and this collection is
     * nonempty.)
     *
     * @param c collection containing elements to be added to this collection
     * @return the sequence list
     * @throws NullPointerException if the specified collection contains a
     *         null element and this collection does not permit null elements,
     *         or if the specified collection is null
     * @see #push(Object)
     */
    List<Long> pushAll(Collection<? extends E> c);

    /**
     * Retrieves, but does not remove, the head of this queue,
     * or returns {@code null} if this queue is empty.
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    QueueElement<E> peek();

    /**
     * Retrieves, but does not remove, the head of this queue,
     * or returns {@code null} if this queue is empty.
     *
     * @param size get the size of this queue
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    List<QueueElement<E>> peek(int size);

    /**
     * Returns an iterator over the elements in this list in proper sequence.
     *
     * @return an iterator over the elements in this list in proper sequence
     */
    @Override
    Iterator<QueueElement<E>> iterator();

    /**
     * Returns the element at the specified position in this list.
     *
     * @param sequence the sequence of the element to return
     * @return the element at the specified position in this list
     */
    E get(Long sequence);

    /**
     * Returns the element at the specified position in this list.
     *
     * @param sequenceList the sequence of the element to return
     * @return the element at the specified position in this list
     */
    Map<Long, E> getAll(List<Long> sequenceList);

    /**
     * Replaces the element at the specified position in this list with the
     * specified element (optional operation).
     *
     * @param sequence the sequence of the element to replace
     * @param element element to be stored at the specified position
     * @return <tt>true</tt> if this list replace the specified element
     * @throws NullPointerException if the specified element is null and
     *         this list does not permit null elements
     */
    boolean set(Long sequence, E element);

    /**
     * Removes the element at the specified position in this list (optional
     * operation).  Shifts any subsequent elements to the left (subtracts one
     * from their indices).  Returns the element that was removed from the
     * list.
     *
     * @param sequence the sequence of the element to remove
     * @return <tt>true</tt> if this list remove the specified element
     */
    boolean remove(Long sequence);

    /**
     * Removes the element at the specified position in this list (optional
     * operation).  Shifts any subsequent elements to the left (subtracts one
     * from their indices).  Returns the element that was removed from the
     * list.
     *
     * @param sequenceList the sequence of the element to remove
     */
    void removeAll(List<Long> sequenceList);

    /**
     * Delete queue
     */
    void delete();
}
