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

let src = Logs.Src.create "irmin.pack" ~doc:"Irmin in-memory store"
module Log = (val Logs.src_log src : Logs.LOG)

module type IO = sig
  val open_file: string -> Cstruct.t
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
