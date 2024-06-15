use std::{
    fs::File,
    io::IoSlice,
    mem::MaybeUninit,
    path::Path,
    sync::{mpsc, Arc, Mutex},
};

use zlib_rs::{
    deflate::{DeflateConfig, Method, Strategy},
    DeflateFlush, InflateFlush, ReturnCode,
};

use libz_sys as libz_ng_sys;

fn compress_slice_with_flush_ng<'a>(
    output: &'a mut [u8],
    input: &[u8],
    config: zlib_rs::deflate::DeflateConfig,
    final_flush: DeflateFlush,
) -> (&'a mut [u8], ReturnCode) {
    use core::ffi::{c_char, c_int, c_uint};

    let mut stream = libz_ng_sys::z_stream {
        next_in: input.as_ptr() as *mut u8,
        avail_in: 0, // for special logic in the first  iteration
        total_in: 0,
        next_out: output.as_mut_ptr(),
        avail_out: 0, // for special logic on the first iteration
        total_out: 0,
        msg: core::ptr::null_mut(),
        state: core::ptr::null_mut(),
        zalloc: zlib_rs::allocate::zalloc_c,
        zfree: zlib_rs::allocate::zfree_c,
        opaque: core::ptr::null_mut(),
        data_type: 0,
        adler: 0,
        reserved: 0,
    };

    const STREAM_SIZE: c_int = core::mem::size_of::<libz_ng_sys::z_stream>() as c_int;

    let err = unsafe {
        libz_ng_sys::deflateInit2_(
            &mut stream,
            config.level,
            config.method as i32,
            config.window_bits,
            config.mem_level,
            config.strategy as i32,
            libz_ng_sys::zlibVersion(),
            STREAM_SIZE,
        )
    };

    if err != libz_ng_sys::Z_OK {
        return (&mut [], ReturnCode::from(err));
    }

    let max = c_uint::MAX as usize;

    let mut left = output.len();
    let mut source_len = input.len();

    loop {
        if stream.avail_out == 0 {
            stream.avail_out = Ord::min(left, max) as _;
            left -= stream.avail_out as usize;
        }

        if stream.avail_in == 0 {
            stream.avail_in = Ord::min(source_len, max) as _;
            source_len -= stream.avail_in as usize;
        }

        let flush = if source_len > 0 {
            DeflateFlush::NoFlush
        } else {
            final_flush
        };

        let err = unsafe { libz_ng_sys::deflate(&mut stream, flush as i32) };

        if err != libz_ng_sys::Z_OK {
            break;
        }
    }

    // may DataError if there was insufficient output space
    let err = unsafe { libz_ng_sys::deflateEnd(&mut stream) };
    let return_code: ReturnCode = ReturnCode::from(err);

    (&mut output[..stream.total_out as usize], return_code)
}

fn uncompress_slice_ng<'a>(
    output: &'a mut [u8],
    input: &[u8],
    config: zlib_rs::inflate::InflateConfig,
) -> (&'a mut [u8], ReturnCode) {
    let mut stream = libz_ng_sys::z_stream {
        next_in: input.as_ptr() as *mut u8,
        avail_in: input.len() as _,
        total_in: 0,
        next_out: output.as_mut_ptr(),
        avail_out: output.len() as _,
        total_out: 0,
        msg: std::ptr::null_mut(),
        state: std::ptr::null_mut(),
        zalloc: zlib_rs::allocate::zalloc_c,
        zfree: zlib_rs::allocate::zfree_c,
        opaque: std::ptr::null_mut(),
        data_type: 0,
        adler: 0,
        reserved: 0,
    };

    let dest_len = output.len();
    let mut dest_len_ptr = 0;

    // z_uintmax_t len, left;
    let mut left;
    let dest;
    let buf: &mut [u8] = &mut [1]; /* for detection of incomplete stream when *destLen == 0 */

    let mut len = input.len() as u64;
    if dest_len != 0 {
        left = dest_len as u64;
        dest_len_ptr = 0;
        dest = output.as_mut_ptr();
    } else {
        left = 1;
        dest = buf.as_mut_ptr();
    }

    let err = unsafe {
        libz_ng_sys::inflateInit2_(
            &mut stream,
            config.window_bits,
            libz_ng_sys::zlibVersion(),
            std::mem::size_of::<libz_ng_sys::z_stream>() as i32,
        )
    };
    if err != ReturnCode::Ok as _ {
        return (&mut [], ReturnCode::from(err));
    }

    stream.next_out = dest;
    stream.avail_out = 0;

    let err = loop {
        if stream.avail_out == 0 {
            stream.avail_out = Ord::min(left, u32::MAX as u64) as u32;
            left -= stream.avail_out as u64;
        }

        if stream.avail_out == 0 {
            stream.avail_in = Ord::min(len, u32::MAX as u64) as u32;
            len -= stream.avail_in as u64;
        }

        let err = unsafe { libz_ng_sys::inflate(&mut stream, InflateFlush::NoFlush as _) };
        let err = ReturnCode::from(err);

        if err != ReturnCode::Ok as _ {
            break err;
        }
    };

    if dest_len != 0 {
        dest_len_ptr = stream.total_out;
    } else if stream.total_out != 0 && err == ReturnCode::BufError as _ {
        left = 1;
    }

    unsafe { libz_ng_sys::inflateEnd(&mut stream) };

    let ret = match err {
        ReturnCode::StreamEnd => ReturnCode::Ok,
        ReturnCode::NeedDict => ReturnCode::DataError,
        ReturnCode::BufError if (left + stream.avail_out as u64) != 0 => ReturnCode::DataError,
        _ => err,
    };

    // SAFETY: we have now initialized these bytes
    let output_slice = unsafe {
        std::slice::from_raw_parts_mut(output.as_mut_ptr() as *mut u8, dest_len_ptr as usize)
    };

    if !stream.msg.is_null() {
        dbg!(unsafe { std::ffi::CStr::from_ptr(stream.msg) });
    }

    (output_slice, ret)
}

pub const MAX_WBITS: i32 = 15; // 32kb LZ77 window
const MAX_MEM_LEVEL: i32 = 9;
const DEF_MEM_LEVEL: i32 = if MAX_MEM_LEVEL > 8 { 8 } else { MAX_MEM_LEVEL };

fn process_chunk(
    chunk: &[u8],
    dictionary: &[u8],
    level: i32, // 0..=9
) -> Vec<u8> {
    debug_assert_eq!((1usize << 15).trailing_zeros(), 15);
    let window_bits = dictionary.len().trailing_zeros();
    let window_bits = 15;

    let config = zlib_rs::deflate::DeflateConfig {
        level,
        method: Method::Deflated,
        window_bits: -1 * window_bits as i32,
        mem_level: DEF_MEM_LEVEL,
        strategy: Strategy::Default,
    };

    // this should just always be enough.
    let mut output = vec![0; chunk.len() + 1024];

    let (prefix, err) = 'blk: {
        let flush = DeflateFlush::SyncFlush;
        let output_uninit = unsafe {
            core::slice::from_raw_parts_mut(
                output.as_mut_ptr() as *mut MaybeUninit<u8>,
                output.len(),
            )
        };

        {
            use zlib_rs::{
                c_api::z_stream,
                deflate::{deflate, end, init, DeflateStream},
            };

            let mut stream = z_stream {
                next_in: chunk.as_ptr() as *mut u8,
                avail_in: 0, // for special logic in the first  iteration
                total_in: 0,
                next_out: output_uninit.as_mut_ptr() as *mut u8,
                avail_out: 0, // for special logic on the first iteration
                total_out: 0,
                msg: core::ptr::null_mut(),
                state: core::ptr::null_mut(),
                zalloc: None,
                zfree: None,
                opaque: core::ptr::null_mut(),
                data_type: 0,
                adler: 0,
                reserved: 0,
            };

            let err = init(&mut stream, config);
            if err != ReturnCode::Ok {
                break 'blk (&mut [][..], err);
            }

            let err = if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                zlib_rs::deflate::set_dictionary(stream, dictionary)
            } else {
                ReturnCode::StreamError
            };

            if err != ReturnCode::Ok {
                break 'blk (&mut [][..], err);
            }

            let max = core::ffi::c_uint::MAX as usize;

            let mut left = output_uninit.len();
            let mut source_len = chunk.len();

            loop {
                if stream.avail_out == 0 {
                    stream.avail_out = Ord::min(left, max) as _;
                    left -= stream.avail_out as usize;
                }

                if stream.avail_in == 0 {
                    stream.avail_in = Ord::min(source_len, max) as _;
                    source_len -= stream.avail_in as usize;
                }

                let flush = if source_len > 0 {
                    DeflateFlush::NoFlush
                } else {
                    flush
                };

                let err =
                    if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                        deflate(stream, flush)
                    } else {
                        ReturnCode::StreamError
                    };

                if err != ReturnCode::Ok {
                    break;
                }
            }

            // SAFETY: we have now initialized these bytes
            let output_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    output_uninit.as_mut_ptr() as *mut u8,
                    stream.total_out as usize,
                )
            };

            // may DataError if insufficient output space
            let return_code =
                if let Some(stream) = unsafe { DeflateStream::from_stream_mut(&mut stream) } {
                    match end(stream) {
                        Ok(_) => ReturnCode::Ok,
                        Err(_) => ReturnCode::DataError,
                    }
                } else {
                    ReturnCode::Ok
                };

            (output_slice, return_code)
        }
    };
    assert_eq!(err, ReturnCode::DataError);

    let n = prefix.len();

    output.truncate(n);

    output
}

fn do_the_thing(input: &[u8]) -> Vec<u8> {
    let mut result = vec![0; 128];

    let window_bits = crate::MAX_WBITS;

    let config = zlib_rs::deflate::DeflateConfig {
        level: 6,
        method: Method::Deflated,
        window_bits: 16 + window_bits, // use gzip
        mem_level: DEF_MEM_LEVEL,
        strategy: Strategy::Default,
    };

    let inflate_config = zlib_rs::inflate::InflateConfig {
        window_bits: 16 + window_bits, // gzip was used
    };

    // write the correct gzip header into our result
    // let (_, err) = zlib_rs::deflate::compress_slice_with_flush(&mut result, b"", config, DeflateFlush::Finish);
    let (_, err) = compress_slice_with_flush_ng(&mut result, b"", config, DeflateFlush::Finish);
    assert_eq!(err, ReturnCode::Ok);

    // size of the gzip header
    result.truncate(10);

    let chunk_size = 1 << 17;
    let chunk_count = input.len().div_ceil(chunk_size);

    let chunks = input.chunks(chunk_size);
    let windows = std::iter::once(&[][..]).chain(
        input
            .chunks(chunk_size)
            .map(|c| &c[(c.len() - (1 << window_bits))..]),
    );

    use std::sync::mpsc::channel;
    let (sender, receiver) = channel();

    let result = Arc::new(Mutex::new(result));

    rayon::ThreadPoolBuilder::new()
        .num_threads(14)
        .build_global()
        .unwrap();

    rayon::scope(|spawner| {
        for (i, (chunk, window)) in chunks.zip(windows).enumerate() {
            let sender = sender.clone();
            spawner.spawn(move |_| {
                let deflated = process_chunk(chunk, window, config.level);

                sender.send((i, deflated)).unwrap();
            });
        }

        let result = result.clone();
        spawner.spawn(move |_| {
            let mut written_count = 0;
            let mut store: Vec<Option<Vec<u8>>> = vec![None; chunk_count];

            for _ in 0..chunk_count {
                let (i, chunk) = receiver.recv().unwrap();

                if i == written_count {
                    let mut slices = Vec::with_capacity(16);

                    slices.push(IoSlice::new(chunk.as_slice()));

                    let mut it = store[written_count + 1..].iter();
                    while let Some(Some(chunk)) = it.next() {
                        slices.push(IoSlice::new(chunk.as_slice()));
                    }

                    use std::io::Write;
                    result.try_lock().unwrap().write_vectored(&slices).unwrap();

                    written_count += slices.len();
                    slices.clear();
                } else {
                    let _ = store[i].insert(chunk);
                }
            }
        });
    });

    let mut result = Arc::try_unwrap(result).unwrap().into_inner().unwrap();

    // add a final block
    {
        let mut output = [0; 64];
        let (prefix, err) = zlib_rs::deflate::compress_slice_with_flush(
            &mut output,
            b"",
            config,
            DeflateFlush::Finish,
        );
        assert_eq!(err, ReturnCode::Ok);

        result.extend(&prefix[10..]);
    }

    let crc = zlib_rs::crc32(0, input);

    // write the trailer
    result.truncate(result.len() - 8);
    result.extend(crc.to_le_bytes());
    result.extend((input.len() as u32).to_le_bytes());

    if true {
        let mut output = vec![0; input.len()];

        // let (output, err) = zlib_rs::inflate::uncompress_slice(&mut output, &result, inflate_config);
        let (output, err) = uncompress_slice_ng(&mut output, &result, inflate_config);
        assert_eq!(err, ReturnCode::Ok);

        assert_eq!(output, input);
    }

    result
}

enum Job<'a> {
    CalculateAdler32 {
        index: usize,
        data: &'a [u8],
    },
    CalculateCrc32 {
        index: usize,
        data: &'a [u8],
    },
    Compress {
        index: usize,
        data: &'a [u8],
        window: &'a [u8],
        config: DeflateConfig,
        output_buffer: Vec<u8>,
    },
}

enum Complete {
    Adler32 {
        index: usize,
        checksum: (usize, u32),
    },
    Crc32 {
        index: usize,
        checksum: (usize, u32),
    },
    Compressed {
        index: usize,
        output_buffer: Vec<u8>,
    },
}

fn worker(receiver: spmc::Receiver<Job>, sender: mpsc::Sender<Complete>) {
    while let Ok(job) = receiver.recv() {
        match job {
            Job::CalculateAdler32 { index, data } => {
                let crc = zlib_rs::adler32(1, data);

                let complete = Complete::Adler32 {
                    index,
                    checksum: (data.len(), crc as u32),
                };

                sender.send(complete).unwrap();
            }
            Job::CalculateCrc32 { index, data } => {
                let crc = zlib_rs::crc32(0, data);

                let complete = Complete::Crc32 {
                    index,
                    checksum: (data.len(), crc as u32),
                };

                sender.send(complete).unwrap();
            }
            Job::Compress {
                index,
                data,
                window,
                config,
                output_buffer: _,
            } => {
                let deflated = process_chunk(data, window, config.level);

                let complete = Complete::Compressed {
                    index,
                    output_buffer: deflated,
                };

                sender.send(complete).unwrap();
            }
        }
    }
}

// thread that performs the writing of the output
fn writer(
    mut file: File,
    chunk_count: usize,
    config: DeflateConfig,
    receiver: mpsc::Receiver<Complete>,
) -> std::io::Result<()> {
    use std::io::Write;

    let file_type = Type::from_config(&config);

    if matches!(file_type, Type::Gzip) {
        // write the correct gzip header into our result
        let mut header_buffer = [0; 64];
        let (_, err) = zlib_rs::deflate::compress_slice_with_flush(
            &mut header_buffer,
            b"",
            config,
            DeflateFlush::Finish,
        );
        assert_eq!(err, ReturnCode::Ok);

        // size of the gzip header
        file.write_all(&header_buffer[..10])?;
    }

    let mut chunks = vec![None; chunk_count];
    let mut checksums = vec![(0, 0); chunk_count];

    let mut next_chunk_to_write = 0;

    while let Ok(complete) = receiver.recv() {
        match complete {
            Complete::Adler32 { index, checksum } => {
                debug_assert_eq!(checksums[index], (0, 0));
                checksums[index] = checksum;
            }
            Complete::Crc32 { index, checksum } => {
                debug_assert_eq!(checksums[index], (0, 0));
                checksums[index] = checksum;
            }
            Complete::Compressed {
                index,
                output_buffer: chunk,
            } => {
                if index != next_chunk_to_write {
                    chunks[index] = Some(chunk);
                } else {
                    // TODO remove allocation
                    let mut io_slices = Vec::with_capacity(16);

                    io_slices.push(IoSlice::new(chunk.as_slice()));

                    let mut it = chunks[next_chunk_to_write + 1..].iter();
                    while let Some(Some(chunk)) = it.next() {
                        io_slices.push(IoSlice::new(chunk.as_slice()));
                    }

                    file.write_vectored(&io_slices)?;

                    next_chunk_to_write += io_slices.len();
                    io_slices.clear();
                }
            }
        }

        if next_chunk_to_write == chunk_count {
            if checksums.iter().all(|x| *x != (0, 0)) {
                // we've received all checksums
                break;
            }
        }
    }

    // add a final block
    let mut trailer = Vec::new();

    let mut output = [0; 64];
    let (prefix, err) =
        zlib_rs::deflate::compress_slice_with_flush(&mut output, b"", config, DeflateFlush::Finish);
    assert_eq!(err, ReturnCode::Ok);

    trailer.extend(&prefix[10..]);

    trailer.truncate(trailer.len() - 8);

    // write the checksum (if required)
    match file_type {
        Type::Raw => { /* raw deflate does not calculate a checksum */ }
        Type::Standard => {
            let mut len = 0;
            let mut adler1 = 1;
            for (len2, adler2) in checksums {
                adler1 = zlib_rs::adler32_combine(adler1, adler2, len2 as u64);
                len += len2;
            }

            trailer.extend(&adler1.to_le_bytes());
            trailer.extend(&(len as u32).to_le_bytes());
        }
        Type::Gzip => {
            let mut len = 0;
            let mut crc1 = 0;
            for (len2, crc2) in checksums {
                crc1 = zlib_rs::crc32_combine(crc1, crc2, len2 as u64);
                len += len2;
            }

            trailer.extend(&crc1.to_le_bytes());
            trailer.extend(&(len as u32).to_le_bytes());
        }
    }

    file.write_all(&trailer)?;

    Ok(())
}

enum Type {
    Raw,
    Standard,
    Gzip,
}

impl Type {
    fn from_config(deflate_config: &DeflateConfig) -> Self {
        if (1..16).contains(&deflate_config.window_bits) {
            Type::Standard
        } else if (16..).contains(&deflate_config.window_bits) {
            Type::Gzip
        } else {
            Type::Raw
        }
    }
}

const CHUNK_SIZE: usize = 128 * 1024;

fn compress_file(input_path: &Path, deflate_config: DeflateConfig) -> std::io::Result<()> {
    let input_file = File::open(input_path)?;
    let mmap = unsafe { memmap2::Mmap::map(&input_file) }?;

    let mut output_path = input_path.to_owned();
    let new_extension = match output_path.extension() {
        Some(ext) => format!("{}.gz", ext.to_string_lossy()),
        None => "gz".to_string(),
    };
    output_path.set_extension(new_extension);
    let output_file = File::options().write(true).create(true).open(output_path)?;

    let threads = 8;
    let chunk_count = mmap.len().div_ceil(CHUNK_SIZE);

    let (mut job_sender, job_receiver) = spmc::channel();
    let (complete_sender, complete_receiver) = mpsc::channel();

    let chunks = mmap.chunks(CHUNK_SIZE);

    let windows = mmap
        .chunks_exact(CHUNK_SIZE)
        .map(|chunk| &chunk[chunk.len() - 32 * 1024..]);
    let windows = [&[][..]].into_iter().chain(windows);

    std::thread::scope(|spawner| {
        spawner.spawn(move || writer(output_file, chunk_count, deflate_config, complete_receiver));

        for _ in 0..threads {
            let job_receiver = job_receiver.clone();
            let complete_sender = complete_sender.clone();

            spawner.spawn(move || worker(job_receiver, complete_sender));
        }

        for (index, (data, window)) in chunks.zip(windows).enumerate() {
            let job = Job::Compress {
                index,
                data,
                window,
                config: deflate_config,
                output_buffer: vec![],
            };

            job_sender.send(job).unwrap();

            match Type::from_config(&deflate_config) {
                Type::Raw => { /* raw deflate does not calculate a checksum */ }
                Type::Standard => {
                    let job = Job::CalculateAdler32 { index, data };
                    job_sender.send(job).unwrap();
                }
                Type::Gzip => {
                    let job = Job::CalculateCrc32 { index, data };
                    job_sender.send(job).unwrap();
                }
            }
        }

        // all jobs have been sent
        drop(job_sender);
    });

    Ok(())
}

fn main() {
    let deflate_config = zlib_rs::deflate::DeflateConfig {
        level: 6,
        method: Method::Deflated,
        window_bits: 16 + 15,
        mem_level: DEF_MEM_LEVEL,
        strategy: Strategy::Default,
    };

    compress_file(Path::new("/tmp/silesia-small.tar"), deflate_config).unwrap();

    if false {
        let input = std::fs::read("/tmp/silesia-small.tar").unwrap();
        let result = std::fs::read("/tmp/silesia-small.tar.gz").unwrap();

        let mut output = vec![0; input.len() * 2];

        let inflate_config = zlib_rs::inflate::InflateConfig {
            window_bits: deflate_config.window_bits,
        };

        // let (output, err) = zlib_rs::inflate::uncompress_slice(&mut output, &result, inflate_config);
        let (output, err) = uncompress_slice_ng(&mut output, &result, inflate_config);
        assert_eq!(err, ReturnCode::Ok);

        assert_eq!(output, input);
    }

    println!("all done!");
}
