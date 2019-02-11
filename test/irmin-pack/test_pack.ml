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
  type t = Lwt_unix.file_descr

  let open_file file =
    Lwt_unix.openfile file Unix.[O_CREAT; O_RDWR] 0o644

  let append t ~off buf =
    let buf = Bytes.unsafe_of_string buf in
    let rec aux off len =
      Lwt_unix.write t buf off len >>= fun w ->
      if w = 0 then Lwt.return ()
      else aux (off+w) (len-w)
    in
    Lwt_unix.LargeFile.lseek t off Unix.SEEK_SET >>= fun _ ->
    aux 0 (Bytes.length buf)

  let read t ~off buf =
    let rec aux off len =
      Lwt_unix.read t buf off len >>= fun r ->
      if r = 0 || r = len then Lwt.return ()
      else aux (off+r) (len-r)
    in
    Lwt_unix.LargeFile.lseek t off Unix.SEEK_SET >>= fun _ ->
    aux 0 (Bytes.length buf)

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
  IO.open_file "/tmp/test" >>= fun block ->
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

let misc = "misc", [Alcotest_lwt.test_case "dictionnaries" `Quick test_dict]
