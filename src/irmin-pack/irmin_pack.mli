(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

val config: unit -> Irmin.config

module type IO = sig
  type t

  val append: t -> string -> unit Lwt.t

  (** offset starts after the headers (with version and offset) *)
  val read: t -> off:int64 -> bytes -> unit Lwt.t

  val set_version: t -> char -> unit Lwt.t
  val set_offset: t -> int64 -> unit Lwt.t

  val get_version: t -> char Lwt.t
  val get_offset: t -> int64 Lwt.t
end

module Append_only (IO: IO): Irmin.APPEND_ONLY_STORE_MAKER
module Make (IO: IO): Irmin.S_MAKER
module KV (IO: IO): Irmin.KV_MAKER

module Dict (IO: IO): sig
  type t
  val find: t -> string -> int Lwt.t
  val v: ?fresh:bool -> IO.t -> t Lwt.t
end

module Index (IO: IO) (H: Irmin.Hash.S): sig
  type t
  val v: ?fresh:bool -> IO.t -> t Lwt.t
  val find: t -> H.t -> int64 Lwt.t
  val append: t -> H.t -> int64 -> unit Lwt.t
end
