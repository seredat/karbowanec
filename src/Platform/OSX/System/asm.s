/* Copyright (c) 2005-2006 Russ Cox, MIT */
/* Copyright (c) 2018, 2020 Ariadne Conill <ariadne@dereferenced.org> */

#if defined(__arm64__) || defined(__aarch64__)

#define REG_SZ                   (8)
#define MCONTEXT_GREGS           (184)
#define SP_OFFSET                432
#define PC_OFFSET                440
#define PSTATE_OFFSET            448
#define FPSIMD_CONTEXT_OFFSET    464
#define REG_OFFSET(__reg)        (MCONTEXT_GREGS + ((__reg) * REG_SZ))

.globl _setmcontext
_setmcontext:
	/* restore GPRs */
	ldp	x18, x19, [x0, #REG_OFFSET(18)]
	ldp	x20, x21, [x0, #REG_OFFSET(20)]
	ldp	x22, x23, [x0, #REG_OFFSET(22)]
	ldp	x24, x25, [x0, #REG_OFFSET(24)]
	ldp	x26, x27, [x0, #REG_OFFSET(26)]
	ldp	x28, x29, [x0, #REG_OFFSET(28)]
	ldr	x30,      [x0, #REG_OFFSET(30)]

	/* save current stack pointer */
	ldr	x2, [x0, #SP_OFFSET]
	mov	sp, x2

	add x2, x0, #FPSIMD_CONTEXT_OFFSET
	ldp q8, q9,   [x2, #144]
	ldp q10, q11, [x2, #176]
	ldp q12, q13, [x2, #208]
	ldp q14, q15, [x2, #240]

	/* save current program counter in link register */
	ldr	x16, [x0, #PC_OFFSET]

	/* restore args */
	ldp	x2, x3, [x0, #REG_OFFSET(2)]
	ldp	x4, x5, [x0, #REG_OFFSET(4)]
	ldp	x6, x7, [x0, #REG_OFFSET(6)]
	ldp	x0, x1, [x0, #REG_OFFSET(0)]

	/* jump to new PC */
	br	x16

.globl _getmcontext
_getmcontext:
	str	xzr, [x0, #REG_OFFSET(0)]

	/* save GPRs */
	stp	x0, x1,   [x0, #REG_OFFSET(0)]
	stp	x2, x3,   [x0, #REG_OFFSET(2)]
	stp	x4, x5,   [x0, #REG_OFFSET(4)]
	stp	x6, x7,   [x0, #REG_OFFSET(6)]
	stp	x8, x9,   [x0, #REG_OFFSET(8)]
	stp	x10, x11, [x0, #REG_OFFSET(10)]
	stp	x12, x13, [x0, #REG_OFFSET(12)]
	stp	x14, x15, [x0, #REG_OFFSET(14)]
	stp	x16, x17, [x0, #REG_OFFSET(16)]
	stp	x18, x19, [x0, #REG_OFFSET(18)]
	stp	x20, x21, [x0, #REG_OFFSET(20)]
	stp	x22, x23, [x0, #REG_OFFSET(22)]
	stp	x24, x25, [x0, #REG_OFFSET(24)]
	stp	x26, x27, [x0, #REG_OFFSET(26)]
	stp	x28, x29, [x0, #REG_OFFSET(28)]
	str	x30,      [x0, #REG_OFFSET(30)]

	/* save current program counter in link register */
	str	x30, [x0, #PC_OFFSET]

	/* save current stack pointer */
	mov	x2, sp
	str	x2, [x0, #SP_OFFSET]

	/* save pstate */
	str	xzr, [x0, #PSTATE_OFFSET]

	add x2, x0, #FPSIMD_CONTEXT_OFFSET
	stp q8, q9,   [x2, #144]
	stp q10, q11, [x2, #176]
	stp q12, q13, [x2, #208]
	stp q14, q15, [x2, #240]

	mov	x0, #0
	ret
#else
.globl _setmcontext
_setmcontext:
	movq	16(%rdi), %rsi
	movq	24(%rdi), %rdx
	movq	32(%rdi), %rcx
	movq	40(%rdi), %r8
	movq	48(%rdi), %r9
	movq	56(%rdi), %rax
	movq	64(%rdi), %rbx
	movq	72(%rdi), %rbp
	movq	80(%rdi), %r10
	movq	88(%rdi), %r11
	movq	96(%rdi), %r12
	movq	104(%rdi), %r13
	movq	112(%rdi), %r14
	movq	120(%rdi), %r15
	movq	184(%rdi), %rsp
	pushq	160(%rdi)	/* new %eip */
	movq	8(%rdi), %rdi
	ret

.globl _getmcontext
_getmcontext:
	movq	%rdi, 8(%rdi)
	movq	%rsi, 16(%rdi)
	movq	%rdx, 24(%rdi)
	movq	%rcx, 32(%rdi)
	movq	%r8, 40(%rdi)
	movq	%r9, 48(%rdi)
	movq	$1, 56(%rdi)	/* %rax */
	movq	%rbx, 64(%rdi)
	movq	%rbp, 72(%rdi)
	movq	%r10, 80(%rdi)
	movq	%r11, 88(%rdi)
	movq	%r12, 96(%rdi)
	movq	%r13, 104(%rdi)
	movq	%r14, 112(%rdi)
	movq	%r15, 120(%rdi)

	movq	(%rsp), %rcx	/* %rip */
	movq	%rcx, 160(%rdi)
	leaq	8(%rsp), %rcx	/* %rsp */
	movq	%rcx, 184(%rdi)
	
	movq	32(%rdi), %rcx	/* restore %rcx */
	movq	$0, %rax
	ret
#endif