/*
 * Copyright 2021 Arman Bilge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vancats.data

import cats.Eq

final class Tsil[+A](private val reversed: List[A], val size: Int)
    extends PartialFunction[Int, A] {

  def toList: List[A] = reversed.reverse

  def uncons: Option[(A, Tsil[A])] = Option.when(nonEmpty) {
    (reversed.last, new Tsil(reversed.init, size - 1))
  }

  def initLast: Option[(Tsil[A], A)] = Option.when(nonEmpty) {
    (new Tsil(reversed.tail, size - 1), reversed.head)
  }

  def headOption: Option[A] = reversed.lastOption

  def lastOption: Option[A] = reversed.headOption

  def apply(i: Int): A = reversed(size - i - 1)

  def isDefinedAt(i: Int): Boolean = reversed.isDefinedAt(size - i - 1)

  def isEmpty: Boolean = size == 0

  def nonEmpty: Boolean = !isEmpty

  def indexOf[B >: A: Eq](b: B): Int =
    indexWhere(Eq[B].eqv(_, b))

  def indexWhere(pred: A => Boolean): Int = {
    val i = reversed.lastIndexWhere(pred)
    if (i == -1) -1 else size - i - 1
  }

  def lastIndexOf[B >: A: Eq](b: B): Int =
    lastIndexWhere(Eq[B].eqv(_, b))

  def lastIndexWhere(pred: A => Boolean): Int = {
    val i = reversed.indexWhere(pred)
    if (i == -1) -1 else size - i - 1
  }

  def concat[A2 >: A](t: Tsil[A2]): Tsil[A2] =
    new Tsil(t.reversed ::: reversed, t.size + size)

  def ++[A2 >: A](t: Tsil[A2]): Tsil[A2] =
    concat(t)

  def prepend[A2 >: A](a: A2): Tsil[A2] =
    new Tsil(reversed :+ a, size + 1)

  def +:[A2 >: A](a: A2): Tsil[A2] =
    prepend(a)

  def append[A2 >: A](a: A2): Tsil[A2] =
    new Tsil(a :: reversed, size + 1)

  def :+[A2 >: A](a: A2): Tsil[A2] =
    append(a)

  def take(n: Int): Tsil[A] = {
    val m = size - n
    new Tsil(reversed.drop(m), ((size - m) min size) max 0)
  }

  def takeRight(n: Int): Tsil[A] =
    new Tsil(reversed.take(n), (n min size) max 0)

  def drop(n: Int): Tsil[A] = {
    val m = size - n
    new Tsil(reversed.take(m), (m min size) max 0)
  }

  def dropRight(n: Int): Tsil[A] =
    new Tsil(reversed.drop(n), ((size - n) min size) max 0)

  def slice(from: Int, until: Int): Tsil[A] = if (until > from) {
    val before = size - until - 1
    val to = size - from - 1
    new Tsil(
      reversed.slice(before + 1, to + 1),
      (((to + 1) min size) max 0) - (((before + 1) max 0) min size))
  } else Tsil.empty

  def splitAt(n: Int): (Tsil[A], Tsil[A]) = {
    val splitAfter = size - n - 1
    val m = splitAfter + 1
    val (after, before) = reversed.splitAt(m)
    (new Tsil(before, ((size - m) max 0) min size), new Tsil(after, (m max 0) min size))
  }

  override def toString: String =
    reversed.reverseIterator.mkString("Tsil(", ", ", ")")

  override def equals(obj: Any): Boolean = obj match {
    case that: Tsil[A] @unchecked =>
      this.size == that.size && this.reversed == that.reversed
    case _ => toList.equals(obj)
  }

  override def hashCode(): Int = reversed.hashCode()

}

object Tsil {
  val empty: Tsil[Nothing] = new Tsil(Nil, 0)

  def apply[A](as: A*): Tsil[A] =
    new Tsil(as.view.reverse.toList, as.size)

  def unapplySeq[A](tsil: Tsil[A]): Some[Seq[A]] =
    Some(tsil.toList)

  def fromList[A](l: List[A]): Tsil[A] =
    new Tsil(l.reverse, l.size)

  def fromSeq[A](s: Seq[A]): Tsil[A] =
    new Tsil(s.view.reverse.toList, s.size)
}
