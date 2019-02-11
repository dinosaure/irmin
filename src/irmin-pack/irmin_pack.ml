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

  val append: t -> string -> unit Lwt.t

  (** offset starts after the headers (with version and offset) *)
  val read: t -> off:int64 -> bytes -> unit Lwt.t

  val set_version: t -> char -> unit Lwt.t
  val set_offset: t -> int64 -> unit Lwt.t

  val get_version: t -> char Lwt.t
  val get_offset: t -> int64 Lwt.t
end

module Dict (IO: IO) = struct

  type t = {
    cache : (string, int) Hashtbl.t;
    block : IO.t;
    mutable offset: int64;
  }

  let append_string t v =
    let len = Int32.of_int (String.length v) in
    IO.append t.block Irmin.Type.(encode_bin int32 len) >>= fun () ->
    IO.append t.block v >|= fun () ->
    Int64.add t.offset Int64.(of_int (4 + String.length v))

  let update_offset t = IO.set_offset t.block t.offset

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
      IO.set_version block '\000' >>= fun () ->
      IO.set_offset block 0L >|= fun () ->
      { cache; block; offset = 0L }
    ) else (
      IO.get_offset block >>= fun len ->
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
      aux 0 0L >|= fun () ->
      { cache; block; offset = len }
    )
end

module Index (IO: IO) (H: Irmin.Hash.S) = struct

  let invalid_bounds off len =
    Fmt.invalid_arg "Invalid bounds (off: %d, len: %d)" off len

  type entry = { hash : H.t ; offset : int64 }

  let offset_size = 64 / 8

  module Decoder = struct

    type decode = [ `Await | `End | `Entry of entry | `Malformed of string ]
    type src = [ `Manual ]

    let unexpected_end_of_input = `Malformed "Unexpected end of input"

    type decoder =
      { src : src
      ; mutable i_off : int
      ; mutable i_len : int
      ; mutable i_pos : int
      ; mutable i : Bytes.t
      ; mutable h_len : int
      ; mutable h_need : int
      ; h : Bytes.t
      ; mutable hash : H.t
      ; mutable k : decoder -> decode }

    let end_of_input decoder =
      decoder.i <- Bytes.empty ;
      decoder.i_off <- 0 ;
      decoder.i_pos <- 0 ;
      decoder.i_len <- min_int

    let unsafe_blit src src_off dst dst_off len =
      Bytes.unsafe_blit src src_off dst dst_off len

    let src decoder buffer off len =
      if off < 0 || len < 0 || off + len > Bytes.length buffer
      then invalid_bounds off len ;
      if len = 0 then end_of_input decoder
      else (
        decoder.i <- buffer ;
        decoder.i_off <- off ;
        decoder.i_pos <- 0 ;
        decoder.i_len <- len - 1 )

    let refill k decoder =
      match decoder.src with
      | `Manual -> decoder.k <- k ; `Await

    let ret k value decoder =
      decoder.k <- k ; (value :> decode)
    (* XXX(dinosaure): post-processing is only (:>). *)

    let i_rem decoder = decoder.i_len - decoder.i_pos + 1

    let t_need decoder need =
      decoder.h_len <- 0 ;
      decoder.h_need <- need

    let rec t_fill k decoder =
      let blit decoder len =
        unsafe_blit decoder.i
          (decoder.i_off + decoder.i_pos)
          decoder.h decoder.h_len len ;
        decoder.i_pos <- decoder.i_pos + len ;
        decoder.h_len <- decoder.h_len + len
      in
      let rem = i_rem decoder in
      if rem < 0 then k decoder
      else
        let need = decoder.h_need - decoder.h_len in
        if rem < need then (
          blit decoder rem ;
          refill (t_fill k) decoder )
        else ( blit decoder need ; k decoder )

    let r_hash buffer off len =
      assert (len = H.digest_size);
      match Irmin.Type.decode_bin ~off H.t (Bytes.unsafe_to_string buffer) with
      | Ok x -> x
      | Error (`Msg e) -> Fmt.failwith "r_hash: %s" e

    let get_int64 ~off buf =
      match Irmin.Type.(decode_bin ~exact:false ~off int64) buf with
      | Ok x -> x
      | Error (`Msg e) -> Fmt.failwith "get_int16_be: %s" e

    let r_entry hash buffer off _len =
      let offset = get_int64 ~off (Bytes.unsafe_to_string buffer) in
      `Entry { hash; offset }

    let rec t_decode_hash decoder =
      if decoder.h_len < decoder.h_need
      then ret decode_entry unexpected_end_of_input decoder
      else decode_offset (r_hash decoder.h 0 decoder.h_len) decoder

    and t_decode_offset decoder =
      if decoder.h_len < decoder.h_need
      then ret decode_entry unexpected_end_of_input decoder
      else ret decode_entry (r_entry decoder.hash decoder.h 0 decoder.h_len) decoder

    and decode_offset hash decoder =
      let rem = i_rem decoder in
      if rem <= 0 then
        if rem < 0
        then `End
        else refill decode_entry decoder
      else
        ( t_need decoder offset_size ;
          decoder.hash <- hash ;
          t_fill t_decode_offset decoder )

    and decode_entry decoder =
      let rem = i_rem decoder in
      if rem <= 0 then
        if rem < 0
        then `End
        else refill decode_entry decoder
      else
        ( t_need decoder H.digest_size ;
          t_fill t_decode_hash decoder )

    let decode decoder = decoder.k decoder

    let decoder src =
      let k = decode_entry in
      let i, i_off, i_pos, i_len = match src with
        | `Manual -> (Bytes.empty, 0, 1, 0) in
      { src
      ; i_off
      ; i_pos
      ; i_len
      ; i
      ; hash= H.digest ""
      ; h= Bytes.create H.digest_size
      ; h_need= 0
      ; h_len= 0
      ; k }
  end

  module Encoder = struct
    type dst = [ `Manual ]
    type encode = [ `Await | `End | `Entry of entry ]

    let invalid_encode () = invalid_arg "Expected `Await encode"

    type encoder =
      { dst : dst
      ; mutable o : Bytes.t
      ; mutable o_off : int
      ; mutable o_pos : int
      ; mutable o_len : int
      ; t : Bytes.t
      ; mutable t_pos : int
      ; mutable t_len : int
      ; mutable offset : int64
      ; mutable k : encoder -> encode -> [ `Ok | `Partial ] }

    let o_rem encoder = encoder.o_len - encoder.o_pos + 1

    let dst encoder buffer off len =
      if off < 0 || len < 0 || off + len > Bytes.length buffer
      then invalid_bounds off len ;
      encoder.o <- buffer ;
      encoder.o_off <- off ;
      encoder.o_pos <- 0 ;
      encoder.o_len <- len - 1

    let dst_rem = o_rem

    let partial k encoder = function
      | `Await -> k encoder
      | `Entry _
      | `End -> invalid_encode ()

    let flush k encoder =
      match encoder.dst with
      | `Manual ->
        encoder.k <- partial k ;
        `Partial

    let t_range encoder len =
      encoder.t_pos <- 0 ;
      encoder.t_len <- len

    let unsafe_blit src src_off dst dst_off len =
      Bytes.unsafe_blit src src_off dst dst_off len

    let rec t_flush k encoder =
      let blit encoder len =
        unsafe_blit encoder.t encoder.t_pos encoder.o encoder.o_pos len ;
        encoder.o_pos <- encoder.o_pos + len ;
        encoder.t_pos <- encoder.t_pos + len
      in
      let rem = o_rem encoder in
      let len = encoder.t_len - encoder.t_pos + 1 in
      if rem < len then (
        blit encoder rem ;
        flush (t_flush k) encoder )
      else ( blit encoder len ; k encoder )

    let set_int64 buf off v =
      let _ : string =
        Irmin.Type.encode_bin ~buf:(buf, off) Irmin.Type.int64 v
      in
      ()

    let rec encode_offset encoder =
      let k encoder =
        encoder.k <- encode_entry ;
        `Ok in
      let rem = o_rem encoder in
      if rem < 1
      then flush (fun encoder -> encode_offset encoder) encoder
      else
        let s, j, k =
          if rem < offset_size then (
            t_range encoder offset_size ;
            (encoder.t, 0, t_flush k)
          ) else
            let j = encoder.o_pos in
            encoder.o_pos <- encoder.o_pos + offset_size ;
            (encoder.o, encoder.o_off + j, k) in
        let offset = encoder.offset in
        set_int64 s j offset;
        k encoder

    and encode_entry encoder value =
      let k offset encoder =
        encoder.offset <- offset ;
        encode_offset encoder in
      match value with
      | `Await -> assert false
      | `End -> `Partial
      | `Entry entry ->
        let rem = o_rem encoder in
        if rem < 1
        then flush (fun encoder -> encode_entry encoder value) encoder
        else
          let s, j, k =
            if rem < H.digest_size then (
              t_range encoder H.digest_size ;
              (encoder.t, 0, t_flush (k entry.offset))
            ) else
              let j = encoder.o_pos in
              encoder.o_pos <- encoder.o_pos + H.digest_size ;
              (encoder.o, encoder.o_off + j, k entry.offset) in
          let hash = Irmin.Type.encode_bin H.t entry.hash in
          let hash = Bytes.unsafe_of_string hash in
          Bytes.unsafe_blit hash 0 s j H.digest_size;
          k encoder

    let encoder dst =
      let o, o_off, o_pos, o_len =
        match dst with
        | `Manual -> (Bytes.empty, 1, 0, 0) in
      { dst
      ; o_off
      ; o_pos
      ; o_len
      ; o
      ; t= Bytes.create H.digest_size
      ; t_pos= 1
      ; t_len= 0
      ; offset= 0L
      ; k= encode_entry }

    let encode encoder = encoder.k encoder
  end

  type t =
    { encoder : Encoder.encoder
    ; decoder : Decoder.decoder
    ; cache : (H.t, int64) Hashtbl.t
    ; block : IO.t
    ; mutable i_off : int64
    ; mutable total : int64
    ; i : Bytes.t
    ; o : Bytes.t }

  let v ?(fresh=false) block =
    IO.get_version block >>= fun version ->
    if version <> '\000' then Fmt.failwith "invalid version";
    (if not fresh then IO.get_offset block
     else (
       IO.set_version block '\000' >>= fun () ->
       IO.set_offset block 0L >|= fun () ->
       0L
     )) >|= fun total ->
    { encoder= Encoder.encoder `Manual
    ; decoder= Decoder.decoder `Manual
    ; cache= Hashtbl.create 512
    ; block
    ; i_off= 0L
    ; total
    ; i= Bytes.create 4096
    ; o= Bytes.create 4096 }

  let find t key =
    match Hashtbl.find t.cache key with
    | offset -> Lwt.return offset
    | exception Not_found ->
      let rec go () = match Decoder.decode t.decoder with
        | `Await ->
          IO.read t.block ~off:t.i_off t.i >>= fun () ->
          t.i_off <- Int64.add t.i_off 4096L ;
          Decoder.src t.decoder t.i 0 4096 ;
          go ()
        | `Entry { hash; offset } ->
          Hashtbl.add t.cache hash offset ;
          if Irmin.Type.equal H.t hash key
          then Lwt.return offset
          else go ()
        | `Malformed _ -> assert false
        | `End -> assert false
      in
      go ()

  let append t key value =
    let rec go () =
      match Encoder.encode t.encoder (`Entry { hash= key; offset= value }) with
      | `Partial ->
        let data = Bytes.sub_string t.o 0 (Encoder.dst_rem t.encoder) in
        IO.append t.block data >>= fun () ->
        Encoder.dst t.encoder t.o 0 (Bytes.length t.o) ;
        go ()
      | `Ok ->
        IO.set_offset t.block (Int64.succ t.total) >>= fun () ->
        t.total <- Int64.succ t.total ;
        Lwt.return ()
    in
    go ()

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
