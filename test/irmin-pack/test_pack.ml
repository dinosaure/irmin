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

open Lwt.Infix

module IO = struct

  type t = {
    version: Cstruct.t;
    offset : Cstruct.t;
    fd     : Lwt_unix.file_descr;
  }

  let header = 9L

  let (++) = Int64.add

  let append t buf =
    let buf = Bytes.unsafe_of_string buf in
    let rec aux off len =
      Lwt_unix.write t.fd buf off len >>= fun w ->
      if w = 0 then Lwt.return ()
      else aux (off+w) (len-w)
    in
    aux 0 (Bytes.length buf)

  let read t ~off buf =
    let rec aux off len =
      Lwt_unix.read t.fd buf off len >>= fun r ->
      if r = 0 || r = len then Lwt.return ()
      else aux (off+r) (len-r)
    in
    Lwt_unix.LargeFile.lseek t.fd (header ++ off) Unix.SEEK_SET >>= fun _ ->
    aux 0 (Bytes.length buf)

  let set_version t v = Cstruct.set_char t.version 0 v; Lwt.return ()
  let set_offset t n = Cstruct.BE.set_uint64 t.offset 0 n; Lwt.return ()
  let get_version t = Lwt.return (Cstruct.get_char t.version 0)
  let get_offset t = Lwt.return (Cstruct.BE.get_uint64 t.offset 0)

  let file_size = 400_960_000_000L

  let v file =
    (Lwt_unix.file_exists file >>= function
      | true  -> Lwt.return ()
      | false ->
        Lwt_unix.openfile file Unix.[O_CREAT; O_RDWR] 0o644 >>= fun fd ->
        Lwt_unix.LargeFile.lseek fd file_size Unix.SEEK_SET >>= fun _ ->
        Lwt_unix.write fd (Bytes.of_string "\000") 0 1 >|= fun _ ->
        ()
    ) >>= fun () ->
    Lwt_unix.openfile file Unix.[O_EXCL; O_RDWR] 0o644 >>= fun fd ->
    let buf =
      Unix.map_file (Lwt_unix.unix_file_descr fd) ~pos:0L
        Bigarray.char Bigarray.c_layout false [| 9 |]
      |> Bigarray.array1_of_genarray
      |> Cstruct.of_bigarray
    in
    let version = Cstruct.sub buf 0 1 in
    let offset = Cstruct.sub buf 1 8 in
    let n = Cstruct.BE.get_uint64 offset 0 in
    Lwt_unix.LargeFile.lseek fd (n ++ header) Unix.SEEK_SET >|= fun _ ->
    { version; offset; fd }

end

let store =
  Irmin_test.store (module Irmin_pack.Make(IO)) (module Irmin.Metadata.None)

let config = Irmin_pack.config ()

let clean () =
  let (module S: Irmin_test.S) = store in
  S.Repo.v config >>= fun repo ->
  S.Repo.branches repo >>= Lwt_list.iter_p (S.Branch.remove repo)

let init () = Lwt.return_unit
let stats = None
let suite = { Irmin_test.name = "PACK"; init; clean; config; store; stats }

module Dict = Irmin_pack.Dict(IO)

let test_dict _switch () =
  IO.v "/tmp/test.dict" >>= fun block ->
  Dict.v ~fresh:true block >>= fun dict ->
  Dict.find dict "foo"  >>= fun x1 ->
  Alcotest.(check int) "foo" 0 x1;
  Dict.find dict "foo"  >>= fun x1 ->
  Alcotest.(check int) "foo" 0 x1;
  Dict.find dict "bar"  >>= fun x2 ->
  Alcotest.(check int) "bar" 1 x2;
  Dict.find dict "toto" >>= fun x3 ->
  Alcotest.(check int) "toto" 2 x3;
  Dict.find dict "titiabc" >>= fun x4 ->
  Alcotest.(check int) "titiabc" 3 x4;
  Dict.find dict "foo"  >>= fun x1 ->
  Alcotest.(check int) "foo" 0 x1;
  Dict.v block >>= fun dict2 ->
  Dict.find dict2 "titiabc" >>= fun x4 ->
  Alcotest.(check int) "titiabc" 3 x4;
  Lwt.return ()

module Index = Irmin_pack.Index(IO)(Irmin.Hash.SHA1)

let test_index _switch () =
  IO.v "/tmp/test.index" >>= fun block ->
  Index.v ~fresh:true block >>= fun t ->
  let h1 = Irmin.Hash.SHA1.digest "foo" in
  let o1 = 42L in
  let h2 = Irmin.Hash.SHA1.digest "bar" in
  let o2 = 142L in
  let h3 = Irmin.Hash.SHA1.digest "otoo" in
  let o3 = 10_098L in
  let h4 = Irmin.Hash.SHA1.digest "sdadsadas" in
  let o4 = 8978_232L in
  Lwt_list.iter_s (fun (h, o) -> Index.append t h o) [
    h1, o1;
    h2, o2;
    h3, o3;
    h4, o4;
  ] >>= fun () ->
  Index.find t h1 >>= fun x1 ->
  Alcotest.(check int64) "h1" o1 x1;
  Index.find t h2 >>= fun x2 ->
  Alcotest.(check int64) "h2" o2 x2;
  Index.find t h3 >>= fun x3 ->
  Alcotest.(check int64) "h3" o3 x3;
  Index.find t h4 >>= fun x4 ->
  Alcotest.(check int64) "h4" o4 x4;
  Lwt.return ()


let misc = "misc", [
    Alcotest_lwt.test_case "dict"  `Quick test_dict;
    Alcotest_lwt.test_case "index" `Quick test_index;
  ]
