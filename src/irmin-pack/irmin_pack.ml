(*
 * Copyright (c) 2013-2019 Thomas Gazagnaire <thomas@gazagnaire.org>
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

open Lwt.Infix

let src = Logs.Src.create "irmin.pack" ~doc:"Irmin in-memory store"
module Log = (val Logs.src_log src : Logs.LOG)

module type IO = sig
  type t
  val append: t -> off:int64 -> string -> unit Lwt.t
  val read: t -> off:int64 -> bytes -> unit Lwt.t
end

module Dict (IO: IO) = struct

  type t = {
    cache : (string, int) Hashtbl.t;
    block : IO.t;
    mutable offset: int64;
  }

  let append_string t v =
    let len = Int32.of_int (String.length v) in
    IO.append t.block ~off:t.offset Irmin.Type.(encode_bin int32 len)
    >>= fun () ->
    IO.append t.block ~off:Int64.(add t.offset 4L) v
    >|= fun () ->
    Int64.add t.offset Int64.(of_int (4 + String.length v))

  let update_offset t =
    IO.append t.block ~off:0L Irmin.Type.(encode_bin int64 t.offset)

  let find t v =
    Log.debug (fun l -> l "[dict] find %S" v);
    try Lwt.return (Hashtbl.find t.cache v)
    with Not_found ->
      let id = Hashtbl.length t.cache in
      append_string t v >>= fun offset ->
      t.offset <- offset;
      update_offset t >>= fun () ->
      Hashtbl.add t.cache v id;
      Lwt.return id

  let length64 ~off block =
    let page = Bytes.create 8 in
    IO.read block ~off page >|= fun () ->
    match
      Irmin.Type.(decode_bin ~exact:true int64) (Bytes.unsafe_to_string page)
    with
    | Error (`Msg e) -> Fmt.failwith "length: %s" e
    | Ok n -> n

  let length32 ~off block =
    let page = Bytes.create 4 in
    IO.read block ~off page >|= fun () ->
    match
      Irmin.Type.(decode_bin ~exact:true int32) (Bytes.unsafe_to_string page)
    with
    | Error (`Msg e) -> Fmt.failwith "length: %s" e
    | Ok n -> Int32.to_int n

  let v ?(fresh=false) block =
    let cache = Hashtbl.create 997 in
    if fresh then (
      let buf = Irmin.Type.(encode_bin int64) 0L in
      IO.append block ~off:0L buf >|= fun () ->
      { cache; block; offset = 8L }
    ) else (
      length64 ~off:0L block >>= fun len ->
      let rec aux n offset =
        if offset >= len then Lwt.return ()
        else (
          length32 ~off:offset block >>= fun len ->
          let v = Bytes.create len in
          let off = Int64.add offset 4L in
          IO.read block ~off v >>= fun () ->
          let v = Bytes.unsafe_to_string v in
          Hashtbl.add cache v  n;
          let off = Int64.add off (Int64.of_int (String.length v)) in
          aux (n+1) off
        ) in
      aux 0 8L >|= fun () ->
      { cache; block; offset = len }
    )
end

module Read_only (IO: IO) (K: Irmin.Type.S) (V: Irmin.Type.S) = struct
  type key = K.t
  type value = V.t
  type 'a t = unit
  let v _config = Lwt.return ()

  let cast t = (t :> [`Read | `Write] t)
  let batch t f = f (cast t)

  let pp_key = Irmin.Type.pp K.t

  let find _t key =
    Log.debug (fun f -> f "find %a" pp_key key);
    failwith "TODO"

  let mem _t key =
    Log.debug (fun f -> f "mem %a" pp_key key);
    failwith "TODO"

end

module Append_only (IO: IO) (K: Irmin.Type.S) (V: Irmin.Type.S) = struct

  include Read_only(IO)(K)(V)

  let add _t key _value =
    Log.debug (fun f -> f "add -> %a" pp_key key);
    failwith "TODO"

end

let config () = Irmin.Private.Conf.empty

module Make (IO: IO) = Irmin.Make(Irmin.Content_addressable(Append_only(IO)))(Irmin_mem.Atomic_write)

module KV (IO: IO) (C: Irmin.Contents.S) =
  Make(IO)
    (Irmin.Metadata.None)
    (C)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Irmin.Hash.SHA1)
